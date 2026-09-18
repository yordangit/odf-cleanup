#!/usr/bin/env python3
"""Compares OpenShift namespaces with ODF RBD images to identify orphaned lab GUIDs.

This script discovers:
1. Active lab GUIDs from OpenShift namespaces (pattern: sandbox-{GUID}-*)
2. Lab GUIDs from ODF RBD images and snapshots
3. Compares them to find orphaned ODF volumes that can be safely cleaned up

Requirements:
- Python packages: pip install kubernetes rados rbd
- Valid kubeconfig with access to Kubernetes/OpenShift cluster
  (needs cluster-scoped read access to VolumeSnapshotContents and
  PersistentVolumes, used to verify parentless csi-snap/csi-vol ownership
  before recommending deletion)
- ODF cluster credentials (CL_CONF, CL_KEYRING environment variables)

Author:  gh:@yordangit
Version: 26.09.17
"""

import rbd
import rados
import os
import re
import urllib3
from typing import List, Dict, Set, Optional
from datetime import datetime
from kubernetes import client, config

# Suppress SSL warnings for kubernetes API calls
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Patterns to extract GUID from ODF image names
CLUSTER_NAME_PREFIXES = ('ocp4-cluster', 'openshift-cluster')
VOLUME_GUID_PATTERN = re.compile(r'(?:' + '|'.join(CLUSTER_NAME_PREFIXES) + r')-([a-z0-9]+)-[a-f0-9-]+')


class OdfOpenShiftComparator:
    """Main class for comparing ODF volumes with OpenShift namespaces"""
    
    def __init__(self, debug: bool = False):
        self.debug = debug
        self.ioctx = None
        self.pool_name = None
        self.cluster = None
        
        # Results
        self.active_namespace_guids: Set[str] = set()
        self.all_namespace_names: Set[str] = set()
        self.odf_guids: Set[str] = set()
        self.orphaned_guids: Set[str] = set()

        # VolumeSnapshotContent objects, used to check real Kubernetes ownership
        # of parentless CSI snapshots (a csi-snap can have zero RBD children and
        # still be actively backing a live VolumeSnapshot). None means "couldn't
        # load them" (fail safe into REVIEW), [] means "loaded, there are none".
        self.volume_snapshot_contents: Optional[List[Dict]] = None

        # Same idea as volume_snapshot_contents, but for parentless csi-vol
        # ownership via PersistentVolume.spec.csi.volumeHandle.
        self.persistent_volumes: Optional[List[Dict]] = None
        
        # Cache for CSI snapshot parent lookups to avoid re-evaluation
        self.csi_snap_guid_cache: Dict[str, Optional[str]] = {}
        self.csi_vol_guid_cache: Dict[str, Optional[str]] = {}
        
        # Track parentless CSI snapshots/volumes and their children analysis
        self.parentless_csi_snaps: Dict[str, Dict] = {}  # {snap_name: {children: [...], child_guids: [...], analysis: ...}}
        self.parentless_csi_vols: Dict[str, Dict] = {}
        
        # Cache expensive operations to avoid repeated RBD calls
        self._cached_ordered_guids: Optional[List[tuple]] = None
        self._cached_all_images: Optional[List[str]] = None
        self._cached_trash_items: Optional[List] = None
        
        # Statistics
        self.stats = {
            'namespaces_found': 0,
            'odf_volumes_found': 0,
            'odf_csi_snaps_found': 0,
            'odf_trash_items_found': 0,
            'unique_odf_guids': 0,
            'active_guids': 0,
            'orphaned_guids': 0
        }
    
    def connect_odf(self) -> bool:
        """Connect to ODF cluster using existing environment variables"""
        try:
            self.pool_name = os.environ['CL_POOL']
            conf_file = os.environ['CL_CONF']
            keyring = os.environ['CL_KEYRING']
            # Extract client name from keyring file
            with open(keyring, 'r') as f:
                for line in f:
                    if line.strip().startswith('[client.') and line.strip().endswith(']'):
                        client_name = line.strip()[1:-1]  # Remove brackets
                        break
                else:
                    raise ValueError(f"No [client.name] found in keyring file: {keyring}")
            self.cluster = rados.Rados(conffile=conf_file, conf=dict(keyring=keyring), name=client_name)
            self.cluster.connect()
            self.ioctx = self.cluster.open_ioctx(self.pool_name)
            
            if self.debug:
                print(f"[v] Connected to ODF cluster: {self.cluster.get_fsid()}")
                print(f"  librados version: {self.cluster.version()}")
                print(f"  Pool: {self.pool_name}")
            
            return True
            
        except KeyError as e:
            print(f"[x] Error: Missing environment variable {e}")
            print("  Required: CL_POOL, CL_CONF, CL_KEYRING")
            return False
        except Exception as e:
            print(f"[x] Error connecting to ODF cluster: {e}")
            return False
    
    def discover_namespace_guids(self) -> bool:
        """Discover active lab GUIDs from OpenShift namespaces"""
        print("Discovering active lab GUIDs from OpenShift namespaces...")
        
        try:
            # Load kubeconfig and create Kubernetes client
            config.load_kube_config()
            v1 = client.CoreV1Api()
            
            if self.debug:
                print(f"  [v] Connected to Kubernetes/OpenShift cluster via kubeconfig")
            
            # Get all namespaces (projects in OpenShift are namespaces)
            namespaces = v1.list_namespace()
            
            if self.debug:
                print(f"  Found {len(namespaces.items)} total namespaces")
            
            # Extract GUIDs from namespace names
            # Pattern: sandbox-{GUID}-* 
            # Extract: {GUID}
            namespace_pattern = re.compile(r'sandbox-([a-z0-9]+)-')
            
            self.stats['namespaces_found'] = len(namespaces.items)
            self.all_namespace_names = {ns.metadata.name for ns in namespaces.items}
            
            for namespace in namespaces.items:
                namespace_name = namespace.metadata.name
                match = namespace_pattern.search(namespace_name)
                if match:
                    guid = match.group(1)
                    self.active_namespace_guids.add(guid)
                    if self.debug:
                        print(f"    Found GUID: {guid} (from namespace: {namespace_name})")
            
            self.stats['active_guids'] = len(self.active_namespace_guids)
            print(f"  Found {self.stats['active_guids']} active lab GUIDs from {self.stats['namespaces_found']} namespaces")
            
            return True
            
        except Exception as e:
            print(f"[x] Error discovering namespaces: {e}")
            print("  Make sure kubeconfig is valid and you have access to the cluster")
            return False

    def discover_volume_snapshot_contents(self) -> bool:
        """Load all VolumeSnapshotContents so we can verify csi-snap ownership.
        Non-fatal on failure - callers fall back to REVIEW."""
        print("Discovering VolumeSnapshotContent objects from OpenShift...")
        try:
            custom_api = client.CustomObjectsApi()
            result = custom_api.list_cluster_custom_object(
                group="snapshot.storage.k8s.io", version="v1", plural="volumesnapshotcontents"
            )
            self.volume_snapshot_contents = result.get('items', [])
            print(f"  Found {len(self.volume_snapshot_contents)} VolumeSnapshotContent objects")
            return True
        except Exception as e:
            print(f"[x] Error discovering VolumeSnapshotContents: {e}")
            print("  Parentless CSI snapshots with no RBD children will be marked REVIEW instead of SAFE TO DELETE")
            self.volume_snapshot_contents = None  # distinguish "couldn't check" from "checked, found none"
            return False

    def discover_persistent_volumes(self) -> bool:
        """Load all PersistentVolumes so we can verify csi-vol ownership.
        Non-fatal on failure - callers fall back to REVIEW."""
        print("Discovering PersistentVolume objects from OpenShift...")
        try:
            core_api = client.CoreV1Api()
            result = core_api.list_persistent_volume()
            self.persistent_volumes = [pv.to_dict() for pv in result.items]
            print(f"  Found {len(self.persistent_volumes)} PersistentVolume objects")
            return True
        except Exception as e:
            print(f"[x] Error discovering PersistentVolumes: {e}")
            print("  Parentless csi-vol images with no RBD children will be marked REVIEW instead of SAFE TO DELETE")
            self.persistent_volumes = None
            return False

    def discover_odf_guids(self) -> bool:
        """Discover all lab GUIDs from ODF RBD images"""
        print("Discovering lab GUIDs from ODF RBD images...")
        
        try:
            # Get all RBD images in pool
            all_images = rbd.RBD().list(self.ioctx)
            # Cache for reuse in counting operations
            self._cached_all_images = all_images
            
            # Get all trash items (convert iterator to list)
            trash_items = list(rbd.RBD().trash_list(self.ioctx))
            # Cache for reuse in counting operations
            self._cached_trash_items = trash_items
            
            if self.debug:
                print(f"  Found {len(all_images)} active images, {len(trash_items)} trash items")
            
            # Process active images
            for img_name in all_images:
                self._extract_guid_from_image(img_name, "active")
            
            # Process trash items
            for item in trash_items:
                self._extract_guid_from_image(item['name'], "trash")
            
            # Report CSI snapshot processing results
            total_csi_snaps = len([name for name in all_images if 'csi-snap' in name])
            cached_csi_snaps = len(self.csi_snap_guid_cache)
            if self.debug and total_csi_snaps > 0:
                print(f"  Processed {cached_csi_snaps} CSI snapshots for parent lookup")
            
            self.stats['unique_odf_guids'] = len(self.odf_guids)
            print(f"  Found {self.stats['unique_odf_guids']} unique lab GUIDs in ODF")
            print(f"    Active volumes: {self.stats['odf_volumes_found']}")
            print(f"    CSI snapshots: {self.stats['odf_csi_snaps_found']}")
            print(f"    Trash items: {self.stats['odf_trash_items_found']}")
            
            return True
            
        except Exception as e:
            print(f"[x] Error discovering ODF images: {e}")
            return False
    
    def _extract_guid_from_image(self, img_name: str, source: str):
        """Extract GUID from an ODF image name"""
        try:
            # Pattern 1: {cluster-prefix}-{GUID}-{UUID}
            # Extract: {GUID}
            match = VOLUME_GUID_PATTERN.search(img_name)
            
            if match:
                guid = match.group(1)
                self.odf_guids.add(guid)
                
                if source == "active":
                    if 'csi-snap' in img_name:
                        self.stats['odf_csi_snaps_found'] += 1
                    else:
                        self.stats['odf_volumes_found'] += 1
                else:  # trash
                    self.stats['odf_trash_items_found'] += 1
                
                if self.debug:
                    print(f"    Found GUID: {guid} (from {source}: {img_name})")
                return guid
            
            # Pattern 2: csi-snap-{UUID} - check parent for GUID
            if 'csi-snap' in img_name and source == "active":
                guid = self._get_guid_from_csi_snap_parent(img_name)
                if guid:
                    self.odf_guids.add(guid)
                    self.stats['odf_csi_snaps_found'] += 1
                    if self.debug:
                        print(f"    Found GUID: {guid} (from CSI snap parent: {img_name})")
                    return guid

            # Pattern 3: csi-vol-{UUID} - check parent for GUID
            if 'csi-vol' in img_name and source == "active":
                guid = self._get_guid_from_csi_vol_parent(img_name)
                if guid:
                    self.odf_guids.add(guid)
                    self.stats['odf_volumes_found'] += 1
                    if self.debug:
                        print(f"    Found GUID: {guid} (from CSI vol parent: {img_name})")
                    return guid
            
            if self.debug and (any(p in img_name for p in CLUSTER_NAME_PREFIXES) or 'csi-snap' in img_name or 'csi-vol' in img_name):
                print(f"    Could not extract GUID from: {img_name}")
                
        except Exception as e:
            if self.debug:
                print(f"    Warning: Error processing {img_name}: {e}")
    
    def _get_guid_from_csi_snap_parent(self, csi_snap_name: str) -> Optional[str]:
        """Get GUID from CSI snapshot's parent image (with caching)"""
        # Check cache first
        if csi_snap_name in self.csi_snap_guid_cache:
            return self.csi_snap_guid_cache[csi_snap_name]
        
        # Not in cache, perform lookup
        guid = None
        try:
            with rbd.Image(self.ioctx, csi_snap_name) as img:
                parent_info = img.parent_info()
                if parent_info and len(parent_info) >= 2:
                    parent_pool, parent_image = parent_info[0], parent_info[1]
                    
                    # Extract GUID from parent image name
                    match = VOLUME_GUID_PATTERN.search(parent_image)
                    if match:
                        guid = match.group(1)
                        
        except Exception as e:
            if self.debug:
                print(f"      Warning: Could not check parent for {csi_snap_name}: {e}")
        
        # Cache the result (even if None)
        self.csi_snap_guid_cache[csi_snap_name] = guid
        
        # If no GUID found (no parent), this might be a parentless CSI snap
        if guid is None and 'csi-snap' in csi_snap_name:
            self._analyze_parentless_csi_snap(csi_snap_name)
        
        return guid

    def _get_guid_from_csi_vol_parent(self, csi_vol_name: str) -> Optional[str]:
        """Get GUID from csi-vol's parent image (with caching)"""
        if csi_vol_name in self.csi_vol_guid_cache:
            return self.csi_vol_guid_cache[csi_vol_name]

        guid = None
        try:
            with rbd.Image(self.ioctx, csi_vol_name) as img:
                parent_info = img.parent_info()
                if parent_info and len(parent_info) >= 2:
                    parent_pool, parent_image = parent_info[0], parent_info[1]
                    match = VOLUME_GUID_PATTERN.search(parent_image)
                    if match:
                        guid = match.group(1)
        except Exception as e:
            if self.debug:
                print(f"      Warning: Could not check parent for {csi_vol_name}: {e}")

        self.csi_vol_guid_cache[csi_vol_name] = guid

        # If no GUID found (no parent), this might be a parentless csi-vol
        if guid is None:
            self._analyze_parentless_csi_vol(csi_vol_name)

        return guid
    
    def _check_csi_snap_k8s_ownership(self, csi_snap_name: str) -> Dict:
        """Match csi-snap's UUID against VSC snapshotHandles to check real ownership.
        Returns {'checked', 'found', 'namespace', 'namespace_active', 'vsc_name'}."""
        result = {'checked': False, 'found': False, 'namespace': None,
                  'namespace_active': None, 'vsc_name': None}

        if self.volume_snapshot_contents is None:
            return result  # couldn't load VSCs at all - stays unchecked, caller should fail safe

        result['checked'] = True

        match = re.search(r'csi-snap-([a-f0-9-]+)', csi_snap_name)
        if not match:
            return result
        snap_uuid = match.group(1)

        for vsc in self.volume_snapshot_contents:
            status = vsc.get('status') or {}
            spec = vsc.get('spec') or {}
            handle = status.get('snapshotHandle') or (spec.get('source') or {}).get('snapshotHandle') or ''
            if snap_uuid in handle:
                result['found'] = True
                result['vsc_name'] = (vsc.get('metadata') or {}).get('name')
                ref = spec.get('volumeSnapshotRef') or {}
                namespace = ref.get('namespace')
                result['namespace'] = namespace
                if namespace:
                    result['namespace_active'] = namespace in self.all_namespace_names
                break

        return result

    def _analyze_parentless_csi_snap(self, csi_snap_name: str):
        """Analyze a parentless CSI snapshot to find children and their GUIDs"""
        if csi_snap_name in self.parentless_csi_snaps:
            return  # Already analyzed
        
        analysis = {
            'children': [],
            'child_guids': [],
            'active_child_guids': [],
            'orphaned_child_guids': [],
            'total_children': 0,
            'has_active_children': False,
            'recommendation': 'unknown',
            'k8s_check': None
        }
        
        try:
            with rbd.Image(self.ioctx, csi_snap_name) as img:
                # Get all descendants (children)
                descendants = list(img.list_descendants())
                analysis['total_children'] = len(descendants)
                
                for desc in descendants:
                    child_name = desc.get('name', '')
                    if child_name:
                        analysis['children'].append(child_name)
                        
                        # Try to extract GUID from child name
                        child_guid = self._extract_guid_from_name(child_name)
                        if child_guid:
                            analysis['child_guids'].append(child_guid)
                            
                            # Check if this GUID is active or orphaned
                            if child_guid in self.active_namespace_guids:
                                analysis['active_child_guids'].append(child_guid)
                                analysis['has_active_children'] = True
                            elif child_guid in self.odf_guids:
                                analysis['orphaned_child_guids'].append(child_guid)
                
                # Determine recommendation
                if analysis['has_active_children']:
                    analysis['recommendation'] = 'KEEP - has active children'
                elif analysis['orphaned_child_guids']:
                    analysis['recommendation'] = 'REVIEW - has orphaned children only'
                elif analysis['total_children'] == 0:
                    # Zero RBD children doesn't mean zero owners - a VolumeSnapshot
                    # can still point at this csi-snap with no clone ever having
                    # been made from it. Verify against real k8s objects.
                    k8s_check = self._check_csi_snap_k8s_ownership(csi_snap_name)
                    analysis['k8s_check'] = k8s_check
                    if not k8s_check['checked']:
                        analysis['recommendation'] = (
                            'REVIEW - no RBD children, but could not verify '
                            'VolumeSnapshotContent ownership (see errors above)'
                        )
                    elif k8s_check['found'] and k8s_check['namespace_active']:
                        analysis['recommendation'] = (
                            f"KEEP - no RBD children, but still referenced by "
                            f"VolumeSnapshotContent {k8s_check['vsc_name']} "
                            f"in active namespace {k8s_check['namespace']}"
                        )
                    elif k8s_check['found']:
                        ns_desc = k8s_check['namespace'] or 'unknown namespace'
                        analysis['recommendation'] = (
                            f"REVIEW - no RBD children, referenced by VolumeSnapshotContent "
                            f"{k8s_check['vsc_name']} in orphaned namespace {ns_desc} "
                            f"(delete the VolumeSnapshotContent too)"
                        )
                    else:
                        analysis['recommendation'] = (
                            'SAFE TO DELETE - no RBD children, no VolumeSnapshotContent reference found'
                        )
                else:
                    analysis['recommendation'] = 'REVIEW - children have no GUID pattern'
                
        except Exception as e:
            if self.debug:
                print(f"      Warning: Could not analyze children for {csi_snap_name}: {e}")
            analysis['recommendation'] = 'ERROR - could not analyze'
        
        self.parentless_csi_snaps[csi_snap_name] = analysis

    def _check_csi_vol_k8s_ownership(self, csi_vol_name: str) -> Dict:
        """Match csi-vol's UUID against PV volumeHandles to check real ownership.
        Returns {'checked', 'found', 'namespace', 'namespace_active', 'pv_name'}."""
        result = {'checked': False, 'found': False, 'namespace': None,
                  'namespace_active': None, 'pv_name': None}

        if self.persistent_volumes is None:
            return result  # couldn't load PVs at all - stays unchecked, caller should fail safe

        result['checked'] = True

        match = re.search(r'csi-vol-([a-f0-9-]+)', csi_vol_name)
        if not match:
            return result
        vol_uuid = match.group(1)

        for pv in self.persistent_volumes:
            spec = pv.get('spec') or {}
            csi = spec.get('csi') or {}
            handle = csi.get('volume_handle') or ''
            if vol_uuid in handle:
                result['found'] = True
                result['pv_name'] = (pv.get('metadata') or {}).get('name')
                claim_ref = spec.get('claim_ref') or {}
                namespace = claim_ref.get('namespace')
                result['namespace'] = namespace
                if namespace:
                    result['namespace_active'] = namespace in self.all_namespace_names
                break

        return result

    def _analyze_parentless_csi_vol(self, csi_vol_name: str):
        """Analyze a parentless csi-vol volume to find children and their GUIDs"""
        if csi_vol_name in self.parentless_csi_vols:
            return  # Already analyzed

        analysis = {
            'children': [],
            'child_guids': [],
            'active_child_guids': [],
            'orphaned_child_guids': [],
            'total_children': 0,
            'has_active_children': False,
            'recommendation': 'unknown',
            'k8s_check': None
        }

        try:
            with rbd.Image(self.ioctx, csi_vol_name) as img:
                descendants = list(img.list_descendants())
                analysis['total_children'] = len(descendants)

                for desc in descendants:
                    child_name = desc.get('name', '')
                    if child_name:
                        analysis['children'].append(child_name)
                        child_guid = self._extract_guid_from_name(child_name)
                        if child_guid:
                            analysis['child_guids'].append(child_guid)
                            if child_guid in self.active_namespace_guids:
                                analysis['active_child_guids'].append(child_guid)
                                analysis['has_active_children'] = True
                            elif child_guid in self.odf_guids:
                                analysis['orphaned_child_guids'].append(child_guid)

                if analysis['has_active_children']:
                    analysis['recommendation'] = 'KEEP - has active children'
                elif analysis['orphaned_child_guids']:
                    analysis['recommendation'] = 'REVIEW - has orphaned children only'
                elif analysis['total_children'] == 0:
                    # Zero RBD children doesn't mean zero owners - a PV can
                    # still point at this csi-vol. Verify against real k8s objects.
                    k8s_check = self._check_csi_vol_k8s_ownership(csi_vol_name)
                    analysis['k8s_check'] = k8s_check
                    if not k8s_check['checked']:
                        analysis['recommendation'] = (
                            'REVIEW - no RBD children, but could not verify '
                            'PersistentVolume ownership (see errors above)'
                        )
                    elif k8s_check['found'] and k8s_check['namespace_active']:
                        analysis['recommendation'] = (
                            f"KEEP - no RBD children, but still referenced by "
                            f"PersistentVolume {k8s_check['pv_name']} "
                            f"in active namespace {k8s_check['namespace']}"
                        )
                    elif k8s_check['found']:
                        ns_desc = k8s_check['namespace'] or 'unknown namespace'
                        analysis['recommendation'] = (
                            f"REVIEW - no RBD children, referenced by PersistentVolume "
                            f"{k8s_check['pv_name']} in orphaned namespace {ns_desc} "
                            f"(delete the PersistentVolume too)"
                        )
                    else:
                        analysis['recommendation'] = (
                            'SAFE TO DELETE - no RBD children, no PersistentVolume reference found'
                        )
                else:
                    analysis['recommendation'] = 'REVIEW - children have no GUID pattern'

        except Exception as e:
            if self.debug:
                print(f"      Warning: Could not analyze children for {csi_vol_name}: {e}")
            analysis['recommendation'] = 'ERROR - could not analyze'

        self.parentless_csi_vols[csi_vol_name] = analysis
    
    def _extract_guid_from_name(self, name: str) -> Optional[str]:
        """Extract GUID from any image name using the standard pattern"""
        match = VOLUME_GUID_PATTERN.search(name)
        return match.group(1) if match else None
    
    def compare_and_find_orphans(self):
        """Compare namespace GUIDs with ODF GUIDs to find orphans"""
        print("\nComparing namespace GUIDs with ODF GUIDs...")
        
        # Find orphaned GUIDs: present in ODF but not in active namespaces
        self.orphaned_guids = self.odf_guids - self.active_namespace_guids
        self.stats['orphaned_guids'] = len(self.orphaned_guids)
        
        print(f"  Active namespace GUIDs: {len(self.active_namespace_guids)}")
        print(f"  ODF GUIDs: {len(self.odf_guids)}")
        print(f"  Orphaned GUIDs: {len(self.orphaned_guids)}")
        
        if self.debug:
            if self.active_namespace_guids:
                print(f"    Active GUIDs: {sorted(self.active_namespace_guids)}")
            if self.odf_guids:
                print(f"    ODF GUIDs: {sorted(self.odf_guids)}")
    
    def generate_report(self):
        """Generate detailed comparison report"""
        print("\n" + "="*80)
        print("ODF-OPENSHIFT COMPARISON REPORT")
        print("="*80)
        print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"ODF Pool: {self.pool_name}")
        print()
        
        # Orphaned GUIDs detail (ordered by complexity)
        if self.orphaned_guids:
            print("ORPHANED GUIDS (ordered by cleanup complexity):")
            ordered_guids = self._order_guids_by_complexity()
            # Cache for reuse in cleanup script generation
            self._cached_ordered_guids = ordered_guids
            
            current_category = None
            for guid, category, counts in ordered_guids:
                if category != current_category:
                    if current_category is not None:
                        print()
                    print(f"  {category.upper()}:")
                    current_category = category
                
                print(f"    {guid}: {counts['total']} items " +
                      f"({counts['volumes']} volumes, {counts['snaps']} snaps, " +
                      f"{counts['trash']} trash)")
        else:
            print("[v] No orphaned GUIDs found - all ODF volumes have active namespaces")
        
        print()
        
        # Parentless CSI snapshots analysis
        if self.parentless_csi_snaps:
            print("PARENTLESS CSI SNAPSHOTS (require manual review):")
            for snap_name, analysis in sorted(self.parentless_csi_snaps.items()):
                print(f"  {snap_name}:")
                print(f"    Children: {analysis['total_children']}")
                if analysis['child_guids']:
                    print(f"    Child GUIDs: {', '.join(analysis['child_guids'])}")
                if analysis['active_child_guids']:
                    print(f"    Active child GUIDs: {', '.join(analysis['active_child_guids'])}")
                if analysis['orphaned_child_guids']:
                    print(f"    Orphaned child GUIDs: {', '.join(analysis['orphaned_child_guids'])}")
                print(f"    Recommendation: {analysis['recommendation']}")
                print()
        else:
            print("[v] No parentless CSI snapshots found")
        
        print()

        # Parentless csi-vol analysis
        if self.parentless_csi_vols:
            print("PARENTLESS CSI VOLUMES (require manual review):")
            for vol_name, analysis in sorted(self.parentless_csi_vols.items()):
                print(f"  {vol_name}:")
                print(f"    Children: {analysis['total_children']}")
                if analysis['child_guids']:
                    print(f"    Child GUIDs: {', '.join(analysis['child_guids'])}")
                if analysis['active_child_guids']:
                    print(f"    Active child GUIDs: {', '.join(analysis['active_child_guids'])}")
                if analysis['orphaned_child_guids']:
                    print(f"    Orphaned child GUIDs: {', '.join(analysis['orphaned_child_guids'])}")
                print(f"    Recommendation: {analysis['recommendation']}")
                print()
        else:
            print("[v] No parentless csi-vol volumes found")

        print()
        
        # Summary at the bottom
        print("SUMMARY:")
        print(f"  Namespaces: {self.stats['namespaces_found']}")
        print(f"  Active Lab GUIDs: {self.stats['active_guids']}")
        print(f"  ODF Volumes: {self.stats['odf_volumes_found']}")
        print(f"  ODF CSI Snapshots: {self.stats['odf_csi_snaps_found']}")
        print(f"  ODF Trash Items: {self.stats['odf_trash_items_found']}")
        print(f"  Unique ODF GUIDs: {self.stats['unique_odf_guids']}")
        print(f"  Orphaned GUIDs: {self.stats['orphaned_guids']}")
        print(f"  Parentless CSI Snapshots: {len(self.parentless_csi_snaps)}")
        print(f"  Parentless CSI Volumes: {len(self.parentless_csi_vols)}")
        
        print("="*80)
    
    def _count_odf_items_for_guid(self, guid: str) -> Dict[str, int]:
        """Count ODF items for a specific GUID"""
        counts = {'volumes': 0, 'snaps': 0, 'trash': 0, 'total': 0}
        
        try:
            # Use cached images if available, otherwise fetch (shouldn't happen in normal flow)
            if self._cached_all_images is not None:
                all_images = self._cached_all_images
            else:
                all_images = rbd.RBD().list(self.ioctx)
                
            for img_name in all_images:
                if guid in img_name:
                    if 'csi-snap' in img_name:
                        counts['snaps'] += 1
                    else:
                        counts['volumes'] += 1
                elif 'csi-snap' in img_name:
                    # Check cached parent GUID (no re-evaluation)
                    cached_guid = self.csi_snap_guid_cache.get(img_name)
                    if cached_guid == guid:
                        counts['snaps'] += 1
                elif 'csi-vol' in img_name:
                    cached_guid = self.csi_vol_guid_cache.get(img_name)
                    if cached_guid == guid:
                        counts['volumes'] += 1
            
            # Use cached trash items if available, otherwise fetch (shouldn't happen in normal flow)
            if self._cached_trash_items is not None:
                trash_items = self._cached_trash_items
            else:
                trash_items = list(rbd.RBD().trash_list(self.ioctx))
                
            for item in trash_items:
                if guid in item['name']:
                    counts['trash'] += 1
            
            counts['total'] = counts['volumes'] + counts['snaps'] + counts['trash']
            
        except Exception as e:
            if self.debug:
                print(f"Warning: Could not count items for GUID {guid}: {e}")
        
        return counts
    
    def _order_guids_by_complexity(self) -> List[tuple]:
        """Order orphaned GUIDs by cleanup complexity (simple to complex)"""
        categorized_guids = {
            'priority 1 - volumes only': [],
            'priority 2 - volumes + snapshots': [],
            'priority 3 - volumes + snapshots + trash': []
        }
        
        for guid in self.orphaned_guids:
            counts = self._count_odf_items_for_guid(guid)
            has_volumes = counts['volumes'] > 0
            has_snaps = counts['snaps'] > 0
            has_trash = counts['trash'] > 0
            
            if has_volumes and not has_snaps and not has_trash:
                category = 'priority 1 - volumes only'
            elif has_volumes and has_snaps and not has_trash:
                category = 'priority 2 - volumes + snapshots'
            elif has_volumes and has_snaps and has_trash:
                category = 'priority 3 - volumes + snapshots + trash'
            elif has_volumes and not has_snaps and has_trash:
                category = 'priority 2 - volumes + snapshots'  # Treat volumes+trash as medium complexity
            elif not has_volumes and has_snaps and not has_trash:
                category = 'priority 1 - volumes only'  # Snapshots only - simple
            elif not has_volumes and not has_snaps and has_trash:
                category = 'priority 1 - volumes only'  # Trash only - simple
            else:
                category = 'priority 3 - volumes + snapshots + trash'  # Mixed/complex cases
            
            categorized_guids[category].append((guid, counts))
        
        # Create ordered list: category, then sorted by GUID within category
        ordered_list = []
        for category in ['priority 1 - volumes only', 'priority 2 - volumes + snapshots', 'priority 3 - volumes + snapshots + trash']:
            for guid, counts in sorted(categorized_guids[category]):
                ordered_list.append((guid, category, counts))
        
        return ordered_list
    
    def generate_cleanup_script(self, output_file: str = "cleanup_orphaned_guids.sh"):
        """Generate bash script for automated cleanup"""
        # Parentless csi-snap/csi-vol images verified SAFE TO DELETE (zero RBD
        # children AND no live k8s reference) - these have no lab GUID, so
        # odf-cleanup.py can't process them. Handled separately via the reaper's
        # CL_CLEANUP_LIST mode, which re-checks watchers/children fresh before
        # removing each one (cluster state can change between analysis and now).
        safe_csi_leftovers = sorted(
            [n for n, a in self.parentless_csi_snaps.items() if a['recommendation'].startswith('SAFE TO DELETE')] +
            [n for n, a in self.parentless_csi_vols.items() if a['recommendation'].startswith('SAFE TO DELETE')]
        )

        if not self.orphaned_guids and not safe_csi_leftovers:
            print("No orphaned GUIDs or safe CSI leftovers found - no cleanup script needed")
            return
        
        print(f"\nGenerating cleanup script: {output_file}")
        
        # Reuse cached ordering result from generate_report() - no expensive RBD calls!
        if self._cached_ordered_guids is None:
            # Fallback if cache not available (shouldn't happen in normal flow)
            ordered_guids = self._order_guids_by_complexity()
        else:
            ordered_guids = self._cached_ordered_guids
            
        priority_1_guids = [guid for guid, cat, counts in ordered_guids if 'priority 1' in cat]
        priority_2_guids = [guid for guid, cat, counts in ordered_guids if 'priority 2' in cat]
        priority_3_guids = [guid for guid, cat, counts in ordered_guids if 'priority 3' in cat]

        csi_leftovers_block = ""
        if safe_csi_leftovers:
            leftover_lines = "\n".join(safe_csi_leftovers)
            csi_leftovers_block = f'''
echo "=== Cleaning up {len(safe_csi_leftovers)} parentless CSI leftover(s) (k8s ownership verified) ==="
CSI_LEFTOVERS_FILE="csi_leftovers_safe_to_delete.txt"
cat > "$CSI_LEFTOVERS_FILE" <<'CSILIST'
{leftover_lines}
CSILIST
unset CL_LAB CL_VOLUME
export CL_CLEANUP_LIST="$CSI_LEFTOVERS_FILE"
python3 "$ODF_REAPER"
echo ""
'''
        
        script_content = f"""#!/bin/bash
# Generated orphaned GUID cleanup script
# Created: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
# Found {len(self.orphaned_guids)} orphaned GUIDs

# Set environment variables (modify as needed)
export CL_POOL="{self.pool_name}"
export CL_CONF="{os.environ.get('CL_CONF', '/path/to/ceph.conf')}"
export CL_KEYRING="{os.environ.get('CL_KEYRING', '/path/to/keyring')}"
export DRY_RUN="false"  # Change to "true" to preview without deleting
export DEBUG="true"

# Locate the other tools regardless of whether this script is run from the
# repo root or from utils/ - avoids needing to copy/move files around.
if [ -f "odf-cleanup.py" ]; then
    ODF_CLEANUP="odf-cleanup.py"
elif [ -f "../odf-cleanup.py" ]; then
    ODF_CLEANUP="../odf-cleanup.py"
else
    echo "[x] Error: could not locate odf-cleanup.py (checked ./ and ../)" >&2
    exit 1
fi

if [ -f "odf-descendant-reaper.py" ]; then
    ODF_REAPER="odf-descendant-reaper.py"
elif [ -f "utils/odf-descendant-reaper.py" ]; then
    ODF_REAPER="utils/odf-descendant-reaper.py"
else
    echo "[x] Error: could not locate odf-descendant-reaper.py (checked ./ and utils/)" >&2
    exit 1
fi

# Orphaned GUIDs to clean up (ordered by complexity: simple → complex)
PRIORITY_1_GUIDS="{' '.join(priority_1_guids)}"
PRIORITY_2_GUIDS="{' '.join(priority_2_guids)}"
PRIORITY_3_GUIDS="{' '.join(priority_3_guids)}"

echo "Starting cleanup of orphaned lab GUIDs..."
echo "Priority 1 (volumes only): $PRIORITY_1_GUIDS"
echo "Priority 2 (volumes + snapshots): $PRIORITY_2_GUIDS" 
echo "Priority 3 (volumes + snapshots + trash): $PRIORITY_3_GUIDS"
echo "DRY_RUN: $DRY_RUN"
echo ""

if [ "$DRY_RUN" != "true" ]; then
    echo "WARNING: DRY_RUN is false - actual deletion will occur."
    read -p "Do you want to proceed? (y/n): " confirm
    if [ "$confirm" != "y" ]; then
        echo "Aborted - no changes made."
        exit 1
    fi
fi

NEEDS_REVIEW_FILE="needs_descendant_review.txt"
rm -f "$NEEDS_REVIEW_FILE"  # start fresh each run - don't accumulate GUIDs across runs

process_guid() {{
    local guid="$1"
    local label="$2"

    echo "=================================================="
    echo "Cleaning up GUID: $guid ($label)"
    echo "=================================================="

    export CL_LAB="$guid"

    if python3 "$ODF_CLEANUP"; then
        echo "[v] Successfully processed GUID: $guid"
    else
        echo "[x] Failed to process GUID: $guid - logged for manual review (try: CL_LAB=$guid python3 $ODF_REAPER)"
        echo "$guid" >> "$NEEDS_REVIEW_FILE"
    fi
}}

# Cleanup loop - Priority 1: Volumes only (safest)
echo "=== PRIORITY 1: Volumes only (safest) ==="
for guid in $PRIORITY_1_GUIDS; do
    process_guid "$guid" "Priority 1 - volumes only"
    echo ""
done

# Cleanup loop - Priority 2: Volumes + snapshots
echo "=== PRIORITY 2: Volumes + snapshots ==="
for guid in $PRIORITY_2_GUIDS; do
    process_guid "$guid" "Priority 2 - volumes + snapshots"
    echo ""
done

# Cleanup loop - Priority 3: Volumes + snapshots + trash (most complex)
echo "=== PRIORITY 3: Volumes + snapshots + trash (most complex) ==="
for guid in $PRIORITY_3_GUIDS; do
    process_guid "$guid" "Priority 3 - volumes + snapshots + trash"
    echo ""
done
{csi_leftovers_block}
echo "Cleanup script completed!"
if [ -f "$NEEDS_REVIEW_FILE" ]; then
    echo ""
    echo "$(wc -l < "$NEEDS_REVIEW_FILE") GUID(s) still need manual review - see $NEEDS_REVIEW_FILE"
fi
"""
        
        try:
            with open(output_file, 'w') as f:
                f.write(script_content)
            
            # Make script executable
            os.chmod(output_file, 0o755)
            
            print(f"[v] Cleanup script created: {output_file}")
            print(f"  Contains {len(self.orphaned_guids)} orphaned GUIDs")
            if safe_csi_leftovers:
                print(f"  Plus {len(safe_csi_leftovers)} parentless csi-snap/csi-vol leftover(s) (k8s ownership verified)")
            print(f"  Run with: ./{output_file}")
            print("  WARNING: DRY_RUN defaults to false - this will actually delete.")
            print("  It will prompt for confirmation before proceeding; set DRY_RUN=\"true\" in the script to preview first.")
            print("  Any GUID odf-cleanup.py can't finish is logged to needs_descendant_review.txt -")
            print("  investigate those manually with: CL_LAB=<guid> python3 odf-descendant-reaper.py")
            
        except Exception as e:
            print(f"[x] Error creating cleanup script: {e}")
    
    def run_comparison(self) -> bool:
        """Main comparison workflow"""
        print("ODF-OpenShift GUID Comparison")
        print("=" * 80)
        
        # Connect to ODF
        if not self.connect_odf():
            return False
        
        try:
            # Discover GUIDs from both sources
            if not self.discover_namespace_guids():
                return False

            # Non-fatal if this fails - parentless csi-snap/csi-vol checks just
            # fall back to REVIEW instead of trusting the RBD-only "no children" signal.
            self.discover_volume_snapshot_contents()
            self.discover_persistent_volumes()

            if not self.discover_odf_guids():
                return False
            
            # Compare and analyze
            self.compare_and_find_orphans()
            
            # Generate reports
            self.generate_report()
            self.generate_cleanup_script()
            
            return True
            
        except Exception as e:
            print(f"[x] Error during comparison: {e}")
            return False
        finally:
            if self.ioctx:
                self.ioctx.close()
            if self.cluster:
                self.cluster.shutdown()


def main():
    """Main entry point"""
    print("ODF-OpenShift GUID Comparator")
    print("=" * 80)
    
    # Check environment variables
    required_envs = ['CL_POOL', 'CL_CONF', 'CL_KEYRING']
    missing_envs = [env for env in required_envs if env not in os.environ]
    
    if missing_envs:
        print(f"[x] Error: Missing environment variables: {', '.join(missing_envs)}")
        print("\nRequired environment variables:")
        for env in required_envs:
            print(f"  {env}")
        print("\nOptional environment variables:")
        print("  DEBUG=[true/false]       - Enable debug output (default: false)")
        print("\nRequired Python packages:")
        print("  pip install kubernetes")
        return 1
    
    # Check debug mode
    debug = os.environ.get('DEBUG', 'false').lower() in ['true', '1', 'yes']
    
    # Show current configuration
    print(f"Configuration:")
    print(f"  Pool: {os.environ['CL_POOL']}")
    print(f"  Debug: {'YES' if debug else 'NO'}")
    print("")
    
    comparator = OdfOpenShiftComparator(debug=debug)
    success = comparator.run_comparison()
    
    return 0 if success else 1


if __name__ == "__main__":
    exit(main())