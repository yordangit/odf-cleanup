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
Version: 26.09.21
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

# Pattern to extract GUID from a k8s namespace name (sandbox-{GUID}-*) - RBD
# namespace names mirror k8s namespace names 1:1, so this doubles as the
# pattern for those too (see discover_odf_guids()'s empty-namespace check).
NAMESPACE_GUID_PATTERN = re.compile(r'sandbox-([a-z0-9]+)-')


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
        self.guid_to_namespaces: Dict[str, List[str]] = {}
        self.odf_guids: Set[str] = set()
        self.orphaned_guids: Set[str] = set()

        # RBD namespaces within the pool
        # Always includes '' (default). odf_guid_namespace tracks
        # which namespace each ODF-side GUID's images were found in.
        self.rbd_namespaces: List[str] = ['']
        self.odf_guid_namespace: Dict[str, str] = {}
        self._current_rbd_namespace: str = ''
        # Orphaned RBD namespaces, no images, no trash, no active lab GUID.
        # Flagged separately for cleanup.
        self.empty_rbd_namespaces: List[str] = []

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

        # Active namespace GUIDs with zero ODF footprint (checked against k8s
        # PVCs to confirm they're genuinely empty, not just missed by the scan).
        self.empty_labs: Dict[str, Dict] = {}
        
        # Cache expensive operations to avoid repeated RBD calls
        self._cached_ordered_guids: Optional[List[tuple]] = None
        self._cached_namespaced_guids: Optional[Dict[str, List[str]]] = None
        self._cached_all_images: Optional[List[tuple]] = None  # [(rbd_namespace, image_name), ...]
        self._cached_trash_items: Optional[List[tuple]] = None  # [(rbd_namespace, trash_item_dict), ...]
        
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

    def discover_rbd_namespaces(self) -> bool:
        """Discover RBD namespaces within the pool, fallback to scanning the default namespace"""
        print("Discovering RBD namespaces in pool...")
        try:
            named = list(rbd.RBD().namespace_list(self.ioctx))
            self.rbd_namespaces = [''] + named
            print(f"  Found {len(named)} named RBD namespace(s), plus the default namespace")
            return True
        except Exception as e:
            print(f"[x] Error listing RBD namespaces: {e}")
            print("  Falling back to the default namespace only - images in named RBD namespaces will be invisible")
            self.rbd_namespaces = ['']
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
            
            self.stats['namespaces_found'] = len(namespaces.items)
            self.all_namespace_names = {ns.metadata.name for ns in namespaces.items}
            
            for namespace in namespaces.items:
                namespace_name = namespace.metadata.name
                match = NAMESPACE_GUID_PATTERN.search(namespace_name)
                if match:
                    guid = match.group(1)
                    self.active_namespace_guids.add(guid)
                    self.guid_to_namespaces.setdefault(guid, []).append(namespace_name)
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
        """Discover all lab GUIDs from ODF RBD images, across every RBD
        namespace in the pool (see discover_rbd_namespaces())."""
        print("Discovering lab GUIDs from ODF RBD images...")

        self._cached_all_images = []
        self._cached_trash_items = []
        total_images = 0
        total_trash = 0

        try:
            for ns in self.rbd_namespaces:
                self.ioctx.set_namespace(ns)
                self._current_rbd_namespace = ns

                all_images = rbd.RBD().list(self.ioctx)
                trash_items = list(rbd.RBD().trash_list(self.ioctx))
                self._cached_all_images.extend((ns, name) for name in all_images)
                self._cached_trash_items.extend((ns, item) for item in trash_items)
                total_images += len(all_images)
                total_trash += len(trash_items)

                # Empty doesn't mean orphaned - an active lab may just have no
                # volume here yet. Only flag it if its own GUID isn't active.
                if ns and not all_images and not trash_items:
                    match = NAMESPACE_GUID_PATTERN.search(ns)
                    ns_guid = match.group(1) if match else None
                    if ns_guid and ns_guid in self.active_namespace_guids:
                        if self.debug:
                            print(f"    RBD namespace '{ns}' is empty but its GUID "
                                  f"({ns_guid}) matches an active OCP namespace - leaving it alone")
                    else:
                        self.empty_rbd_namespaces.append(ns)

                if self.debug:
                    label = ns if ns else '(default)'
                    print(f"  [{label}] {len(all_images)} active images, {len(trash_items)} trash items")

                for img_name in all_images:
                    self._extract_guid_from_image(img_name, "active")
                for item in trash_items:
                    self._extract_guid_from_image(item['name'], "trash")

            self.ioctx.set_namespace('')
            self._current_rbd_namespace = ''

            # Report CSI snapshot processing results
            total_csi_snaps = len([name for ns, name in self._cached_all_images if 'csi-snap' in name])
            cached_csi_snaps = len(self.csi_snap_guid_cache)
            if self.debug and total_csi_snaps > 0:
                print(f"  Processed {cached_csi_snaps} CSI snapshots for parent lookup")

            self.stats['unique_odf_guids'] = len(self.odf_guids)
            print(f"  Found {self.stats['unique_odf_guids']} unique lab GUIDs in ODF "
                  f"(across {len(self.rbd_namespaces)} RBD namespace(s), {total_images} images, {total_trash} trash items)")
            print(f"    Active volumes: {self.stats['odf_volumes_found']}")
            print(f"    CSI snapshots: {self.stats['odf_csi_snaps_found']}")
            print(f"    Trash items: {self.stats['odf_trash_items_found']}")

            return True

        except Exception as e:
            print(f"[x] Error discovering ODF images: {e}")
            self.ioctx.set_namespace('')
            self._current_rbd_namespace = ''
            return False
    
    def _record_odf_guid(self, guid: str):
        """Add guid to odf_guids and remember which RBD namespace it was
        found in (first-seen wins; a lab's images shouldn't span namespaces)."""
        self.odf_guids.add(guid)
        existing = self.odf_guid_namespace.get(guid)
        if existing is None:
            self.odf_guid_namespace[guid] = self._current_rbd_namespace
        elif self.debug and existing != self._current_rbd_namespace:
            print(f"    Warning: GUID {guid} seen in both RBD namespace "
                  f"'{existing}' and '{self._current_rbd_namespace}'")

    def _extract_guid_from_image(self, img_name: str, source: str):
        """Extract GUID from an ODF image name"""
        try:
            # Pattern 1: {cluster-prefix}-{GUID}-{UUID}
            # Extract: {GUID}
            match = VOLUME_GUID_PATTERN.search(img_name)
            
            if match:
                guid = match.group(1)
                self._record_odf_guid(guid)
                
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
                    self._record_odf_guid(guid)
                    self.stats['odf_csi_snaps_found'] += 1
                    if self.debug:
                        print(f"    Found GUID: {guid} (from CSI snap parent: {img_name})")
                    return guid

            # Pattern 3: csi-vol-{UUID} - check parent for GUID
            if 'csi-vol' in img_name and source == "active":
                guid = self._get_guid_from_csi_vol_parent(img_name)
                if guid:
                    self._record_odf_guid(guid)
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
            'k8s_check': None,
            'rbd_namespace': self._current_rbd_namespace,
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
                    elif self.pool_name == 'ocpv-tenants':
                        analysis['recommendation'] = (
                            'REVIEW - no RBD children, no VolumeSnapshotContent reference found, '
                            'but ocpv-tenants is external ODF - cannot verify guest-cluster ownership'
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
            'k8s_check': None,
            'rbd_namespace': self._current_rbd_namespace,
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
                    elif self.pool_name == 'ocpv-tenants':
                        analysis['recommendation'] = (
                            'REVIEW - no RBD children, no PersistentVolume reference found, '
                            'but ocpv-tenants is external ODF - cannot verify guest-cluster ownership'
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

    def check_empty_labs(self):
        """For active namespace GUIDs with zero ODF footprint, check OCP for
        PVCs to confirm they're genuinely empty rather than a scan gap.
        PVCs backed by a pool other than CL_POOL are out of scope."""
        no_footprint_guids = self.active_namespace_guids - self.odf_guids
        if not no_footprint_guids:
            return

        print(f"\nChecking {len(no_footprint_guids)} active lab(s) with no ODF footprint against OCP PVCs...")
        try:
            core_api = client.CoreV1Api()
        except Exception as e:
            print(f"[x] Error creating Kubernetes client: {e}")
            for guid in no_footprint_guids:
                self.empty_labs[guid] = {
                    'namespaces': self.guid_to_namespaces.get(guid, []),
                    'pvcs': [],
                    'status': 'ERROR - could not check PVCs',
                }
            return

        # Index PVs by name once, so each PVC's real backing pool can be
        # looked up via spec.csi.volumeAttributes.pool. None means PVs
        # couldn't be loaded at all - pool lookups just stay unknown.
        pv_by_name = None
        if self.persistent_volumes is not None:
            pv_by_name = {(pv.get('metadata') or {}).get('name'): pv for pv in self.persistent_volumes}

        for guid in no_footprint_guids:
            namespaces = self.guid_to_namespaces.get(guid, [])
            pvcs = []
            list_error = False
            for ns in namespaces:
                try:
                    result = core_api.list_namespaced_persistent_volume_claim(ns)
                except Exception as e:
                    list_error = True
                    if self.debug:
                        print(f"  Warning: could not list PVCs in namespace {ns}: {e}")
                    continue
                for pvc in result.items:
                    pool = None
                    if pv_by_name is not None and pvc.spec.volume_name:
                        pv = pv_by_name.get(pvc.spec.volume_name)
                        if pv:
                            csi = (pv.get('spec') or {}).get('csi') or {}
                            pool = (csi.get('volume_attributes') or {}).get('pool')
                    pvcs.append({'name': pvc.metadata.name, 'namespace': ns,
                                 'phase': pvc.status.phase, 'pool': pool})

            if not pvcs:
                status = 'ERROR - could not check PVCs' if list_error else 'CONFIRMED EMPTY'
            elif any(p['pool'] == self.pool_name for p in pvcs):
                status = 'HAS PVCS - REVIEW (same pool as ODF scan, but no matching GUID found)'
            elif all(p['pool'] is not None and p['pool'] != self.pool_name for p in pvcs):
                status = 'HAS PVCS - DIFFERENT POOL (not scanned by this tool)'
            else:
                status = 'HAS PVCS - REVIEW (could not verify pool for one or more PVCs)'

            self.empty_labs[guid] = {'namespaces': namespaces, 'pvcs': pvcs, 'status': status}

        confirmed = sum(1 for a in self.empty_labs.values() if a['status'] == 'CONFIRMED EMPTY')
        print(f"  Confirmed empty: {confirmed} / {len(no_footprint_guids)}")

    def generate_report(self):
        """Generate detailed comparison report"""
        print("\n" + "="*80)
        print("ODF-OPENSHIFT COMPARISON REPORT")
        print("="*80)
        print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"ODF Pool: {self.pool_name}")
        print()
        
        # Orphaned GUIDs detail (ordered by complexity) - default RBD namespace only
        if self.orphaned_guids:
            ordered_guids = self._order_guids_by_complexity()
            # Cache for reuse in cleanup script generation
            self._cached_ordered_guids = ordered_guids
            namespaced_guids = self._group_namespaced_guids()
            self._cached_namespaced_guids = namespaced_guids

            if ordered_guids:
                print("ORPHANED GUIDS (ordered by cleanup complexity):")
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
                print()

            if namespaced_guids:
                print("ORPHANED GUIDS IN NAMED RBD NAMESPACES:")
                for ns, guids in namespaced_guids.items():
                    print(f"  {ns}:")
                    for guid in guids:
                        counts = self._count_odf_items_for_guid(guid)
                        print(f"    {guid}: {counts['total']} items " +
                              f"({counts['volumes']} volumes, {counts['snaps']} snaps, " +
                              f"{counts['trash']} trash)")
        else:
            print("[v] No orphaned GUIDs found - all ODF volumes have active namespaces")

        if self.empty_rbd_namespaces:
            print(f"\nEMPTY RBD NAMESPACES ({len(self.empty_rbd_namespaces)}, no images/trash - dead weight, will be removed):")
            for ns in sorted(self.empty_rbd_namespaces):
                print(f"  {ns}")

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

        # Active labs with no ODF footprint, checked against OCP PVCs
        if self.empty_labs:
            print("ACTIVE LABS WITH NO ODF FOOTPRINT (checked against OCP PVCs):")
            for guid, info in sorted(self.empty_labs.items()):
                ns_str = ', '.join(info['namespaces']) if info['namespaces'] else '(namespace not found)'
                print(f"  {guid}: {ns_str}")
                print(f"    Status: {info['status']}")
                for pvc in info['pvcs']:
                    pool_str = pvc['pool'] if pvc['pool'] else 'unknown'
                    print(f"      {pvc['name']} ({pvc['phase']}, pool={pool_str})")
            print()

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
        namespaced_count = sum(len(g) for g in (self._cached_namespaced_guids or {}).values())
        if namespaced_count:
            print(f"    (of which {namespaced_count} are in named RBD namespaces)")
        print(f"  Active Labs With No ODF Footprint: {len(self.empty_labs)}")
        print(f"  Parentless CSI Snapshots: {len(self.parentless_csi_snaps)}")
        print(f"  Parentless CSI Volumes: {len(self.parentless_csi_vols)}")
        if self.empty_rbd_namespaces:
            print(f"  Empty RBD Namespaces: {len(self.empty_rbd_namespaces)}")
        
        print("="*80)
    
    def _count_odf_items_for_guid(self, guid: str) -> Dict[str, int]:
        """Count ODF items for a specific GUID"""
        counts = {'volumes': 0, 'snaps': 0, 'trash': 0, 'total': 0}
        
        try:
            # Use cached images if available, otherwise fetch from the default
            # namespace only (shouldn't happen in normal flow - this fallback
            # predates namespace scanning and doesn't cover named namespaces)
            if self._cached_all_images is not None:
                all_images = self._cached_all_images  # [(rbd_namespace, name), ...]
            else:
                all_images = [('', name) for name in rbd.RBD().list(self.ioctx)]

            for _ns, img_name in all_images:
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
            
            # Use cached trash items if available, otherwise fetch from the
            # default namespace only (see note above)
            if self._cached_trash_items is not None:
                trash_items = self._cached_trash_items  # [(rbd_namespace, item), ...]
            else:
                trash_items = [('', item) for item in rbd.RBD().trash_list(self.ioctx)]

            for _ns, item in trash_items:
                if guid in item['name']:
                    counts['trash'] += 1
            
            counts['total'] = counts['volumes'] + counts['snaps'] + counts['trash']
            
        except Exception as e:
            if self.debug:
                print(f"Warning: Could not count items for GUID {guid}: {e}")
        
        return counts
    
    def _group_namespaced_guids(self) -> Dict[str, List[str]]:
        """Orphaned GUIDs living in a named RBD namespace, grouped by namespace.
        Each namespace is provisioner-isolated to one lab, so >1 GUID per
        namespace is flagged as an anomaly, not treated as a normal finding."""
        grouped: Dict[str, List[str]] = {}
        for guid in self.orphaned_guids:
            ns = self.odf_guid_namespace.get(guid, '')
            if ns:
                grouped.setdefault(ns, []).append(guid)
        result = {ns: sorted(guids) for ns, guids in sorted(grouped.items())}

        for ns, guids in result.items():
            if len(guids) > 1:
                print(f"[!] WARNING: RBD namespace '{ns}' has {len(guids)} orphaned GUIDs "
                      f"({', '.join(guids)}) - expected exactly one per namespace "
                      f"(cross-lab contamination or a GUID-extraction bug?) - investigate before running cleanup")

        return result

    def _order_guids_by_complexity(self) -> List[tuple]:
        """Order default-RBD-namespace orphaned GUIDs by cleanup complexity
        (simple to complex). Named-namespace GUIDs are grouped separately
        by _group_namespaced_guids() instead."""
        categorized_guids = {
            'priority 1 - volumes only': [],
            'priority 2 - volumes + snapshots': [],
            'priority 3 - volumes + snapshots + trash': []
        }
        
        for guid in self.orphaned_guids:
            if self.odf_guid_namespace.get(guid, ''):
                continue  # named-namespace GUID, handled separately
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
        # Grouped by RBD namespace since the reaper only targets one namespace
        # per run (CL_RBD_NAMESPACE) - most will be a single default-namespace group.
        safe_csi_leftovers_by_ns: Dict[str, List[str]] = {}
        for n, a in self.parentless_csi_snaps.items():
            if a['recommendation'].startswith('SAFE TO DELETE'):
                safe_csi_leftovers_by_ns.setdefault(a.get('rbd_namespace', ''), []).append(n)
        for n, a in self.parentless_csi_vols.items():
            if a['recommendation'].startswith('SAFE TO DELETE'):
                safe_csi_leftovers_by_ns.setdefault(a.get('rbd_namespace', ''), []).append(n)
        safe_csi_leftovers_by_ns = {ns: sorted(names) for ns, names in sorted(safe_csi_leftovers_by_ns.items())}
        safe_csi_leftovers_total = sum(len(names) for names in safe_csi_leftovers_by_ns.values())

        if not self.orphaned_guids and not safe_csi_leftovers_total and not self.empty_rbd_namespaces:
            print("No orphaned GUIDs, safe CSI leftovers, or empty RBD namespaces found - no cleanup script needed")
            return
        
        print(f"\nGenerating cleanup script: {output_file}")
        
        # Reuse cached ordering results from generate_report() - no expensive RBD calls!
        ordered_guids = self._cached_ordered_guids if self._cached_ordered_guids is not None else self._order_guids_by_complexity()
        namespaced_guids = self._cached_namespaced_guids if self._cached_namespaced_guids is not None else self._group_namespaced_guids()

        priority_1_guids = [guid for guid, cat, counts in ordered_guids if 'priority 1' in cat]
        priority_2_guids = [guid for guid, cat, counts in ordered_guids if 'priority 2' in cat]
        priority_3_guids = [guid for guid, cat, counts in ordered_guids if 'priority 3' in cat]

        namespaced_guids_lines = []
        total_namespaced = sum(len(g) for g in namespaced_guids.values())
        if namespaced_guids:
            namespaced_guids_lines.append(
                f'echo "=== NAMESPACED ORPHANED GUIDS: {total_namespaced} GUID(s) across '
                f'{len(namespaced_guids)} RBD namespace(s) ==="'
            )
            for ns, guids in namespaced_guids.items():
                namespaced_guids_lines.append(f'echo "--- RBD namespace: {ns} ---"')
                for guid in guids:
                    namespaced_guids_lines.append(f'process_namespaced_guid "{guid}" "{ns}"')
        namespaced_guids_block = "\n".join(namespaced_guids_lines)

        empty_namespaces_lines = []
        if self.empty_rbd_namespaces:
            empty_namespaces_lines.append(
                f'echo "=== EMPTY RBD NAMESPACES: {len(self.empty_rbd_namespaces)} (no images/trash) ==="'
            )
            for ns in sorted(self.empty_rbd_namespaces):
                empty_namespaces_lines.append(f'remove_empty_namespace "{ns}"')
        empty_namespaces_block = "\n".join(empty_namespaces_lines)

        csi_leftovers_block = ""
        if safe_csi_leftovers_by_ns:
            blocks = []
            for i, (ns, names) in enumerate(safe_csi_leftovers_by_ns.items(), start=1):
                leftover_lines = "\n".join(names)
                ns_export = f'export CL_RBD_NAMESPACE="{ns}"' if ns else 'unset CL_RBD_NAMESPACE'
                ns_label = f" (RBD namespace: {ns})" if ns else ""
                blocks.append(f'''
echo "=== Cleaning up {len(names)} parentless CSI leftover(s){ns_label} (k8s ownership verified) ==="
CSI_LEFTOVERS_FILE="csi_leftovers_safe_to_delete_{i}.txt"
cat > "$CSI_LEFTOVERS_FILE" <<'CSILIST'
{leftover_lines}
CSILIST
unset CL_LAB CL_VOLUME
{ns_export}
export CL_CLEANUP_LIST="$CSI_LEFTOVERS_FILE"
python3 "$ODF_REAPER"
echo ""
''')
            csi_leftovers_block = "".join(blocks)

        extra_summary_lines = []
        if total_namespaced:
            extra_summary_lines.append(
                f'echo "Namespaced Orphaned GUIDs: {total_namespaced} across {len(namespaced_guids)} RBD namespace(s)"'
            )
        if safe_csi_leftovers_total:
            extra_summary_lines.append(
                f'echo "Parentless CSI leftovers (safe to delete): {safe_csi_leftovers_total}"'
            )
        if self.empty_rbd_namespaces:
            extra_summary_lines.append(
                f'echo "Empty RBD namespaces to remove: {len(self.empty_rbd_namespaces)}"'
            )
        extra_summary_block = "\n".join(extra_summary_lines)

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
{extra_summary_block}
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

# GUIDs living in a named RBD namespace (see discover_rbd_namespaces()) go
# through odf-cleanup.py too, same as process_guid(), just with
# CL_RBD_NAMESPACE also set so it opens the right namespace.
process_namespaced_guid() {{
    local guid="$1"
    local ns="$2"

    echo "=================================================="
    echo "Cleaning up GUID: $guid (RBD namespace: $ns)"
    echo "=================================================="

    export CL_LAB="$guid"
    export CL_RBD_NAMESPACE="$ns"

    if python3 "$ODF_CLEANUP"; then
        echo "[v] Successfully processed GUID: $guid (namespace: $ns)"
    else
        echo "[x] Failed to process GUID: $guid (namespace: $ns) - logged for manual review (try: CL_LAB=$guid CL_RBD_NAMESPACE=$ns python3 $ODF_REAPER)"
        echo "$guid (RBD namespace: $ns)" >> "$NEEDS_REVIEW_FILE"
    fi
    unset CL_RBD_NAMESPACE
}}

# Removes a named RBD namespace that was already empty of images/trash
remove_empty_namespace() {{
    local ns="$1"

    echo "=================================================="
    echo "Removing empty RBD namespace: $ns"
    echo "=================================================="

    unset CL_LAB CL_VOLUME CL_CLEANUP_LIST
    export CL_RBD_NAMESPACE="$ns"

    if python3 "$ODF_REAPER"; then
        echo "[v] $ns removed - confirmed empty, GUID didn't match an active OCP namespace when scanned"
    else
        echo "[x] Failed to remove RBD namespace: $ns - see output above"
        echo "empty RBD namespace: $ns" >> "$NEEDS_REVIEW_FILE"
    fi
    unset CL_RBD_NAMESPACE
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
{namespaced_guids_block}
{empty_namespaces_block}
{csi_leftovers_block}
echo "Cleanup script completed!"
if [ -f "$NEEDS_REVIEW_FILE" ]; then
    echo ""
    echo "$(wc -l < "$NEEDS_REVIEW_FILE") GUID(s) still need manual review - see $NEEDS_REVIEW_FILE"
    echo "Investigate each with: [CL_RBD_NAMESPACE=<ns>] CL_LAB=<guid> python3 $ODF_REAPER"
    echo "Each entry's exact follow-up command was already printed above when it failed"
fi
"""
        
        try:
            with open(output_file, 'w') as f:
                f.write(script_content)
            
            # Make script executable
            os.chmod(output_file, 0o755)
            
            print(f"[v] Cleanup script created: {output_file}")
            print(f"  Contains {len(self.orphaned_guids)} orphaned GUIDs" +
                  (f" ({total_namespaced} in named RBD namespaces)" if total_namespaced else ""))
            if safe_csi_leftovers_total:
                print(f"  Plus {safe_csi_leftovers_total} parentless csi-snap/csi-vol leftover(s) (k8s ownership verified)")
            if self.empty_rbd_namespaces:
                print(f"  Plus {len(self.empty_rbd_namespaces)} empty RBD namespace(s) to remove")
            print(f"  Run with: ./{output_file}")
            print("  WARNING: DRY_RUN defaults to false - this will actually delete.")
            print("  It will prompt for confirmation before proceeding; set DRY_RUN=\"true\" in the script to preview first.")
            
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

            # Non-fatal if this fails - falls back to scanning the default
            # RBD namespace only (pre-existing behavior).
            self.discover_rbd_namespaces()
            if not self.discover_odf_guids():
                return False
            
            # Compare and analyze
            self.compare_and_find_orphans()
            self.check_empty_labs()
            
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