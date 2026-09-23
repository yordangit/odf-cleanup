#!/usr/bin/env python3
"""Deletes ODF objects based on a LAB GUID using a hierarchical tree approach.

Author:  gh:@yordangit
Version: 26.09.18
"""

import rbd
import rados
import struct
import time
import os
from datetime import datetime
from typing import List, Dict, Optional, Set, Tuple
from enum import Enum


class ImageType(Enum):
    VOLUME = "volume"
    CSI_SNAP = "csi-snap"
    INTERNAL_SNAP = "internal-snap"
    TRASH_VOLUME = "trash-volume"
    TRASH_CSI_SNAP = "trash-csi-snap"


class OdfImage:
    """Represents an RBD image (volume or snapshot) in the ODF cluster"""
    def __init__(self, name: str, image_type: ImageType, size: Optional[int] = None,
                 creation_time: Optional[str] = None, parent_name: Optional[str] = None,
                 is_protected: bool = False, in_trash: bool = False, trash_id: Optional[str] = None):
        self.name = name
        self.image_type = image_type
        self.size = size
        self.creation_time = creation_time
        self.parent_name = parent_name
        self.children: List['OdfImage'] = []
        self.internal_snaps: List[str] = []
        self.is_protected = is_protected
        self.in_trash = in_trash
        self.trash_id = trash_id
        # Multi-phase operation metadata
        self.needs_restoration = False
        self.needs_flattening = False
        self.restoration_reason: Optional[str] = None
        self.depends_on_trash = False
        # Rbd_header is missing (Ceph metadata corruption), can't be opened
        # via rbd.Image() at all, needs a rados-level cleanup instead.
        self.phantom_image_id: Optional[str] = None
        # Set when rbd_id/rbd_directory are both missing but rbd.RBD().list2()
        # still resolved a real id - open by this id instead of by name.
        self.open_by_id: Optional[str] = None
    
    def add_child(self, child: 'OdfImage'):
        """Add a child image"""
        if child not in self.children:
            self.children.append(child)
    
    def has_descendants(self) -> bool:
        """Check if image has any descendants"""
        return len(self.children) > 0 or len(self.internal_snaps) > 0
    

class OdfTree:
    """Manages the hierarchical tree of ODF RBD images"""
    
    def __init__(self):
        self.images: Dict[str, OdfImage] = {}
        self.root_images: List[OdfImage] = []
    
    def add_image(self, image: OdfImage):
        """Add an image to the tree"""
        self.images[image.name] = image
        
        # If image has a parent, establish the relationship
        if image.parent_name and image.parent_name in self.images:
            parent = self.images[image.parent_name]
            parent.add_child(image)
        elif not image.parent_name:
            # This is a root image
            self.root_images.append(image)
    
    def build_relationships(self):
        """Build parent-child relationships after all images are added"""
        for image in self.images.values():
            if image.parent_name and image.parent_name in self.images:
                parent = self.images[image.parent_name]
                parent.add_child(image)
            elif not image.parent_name and image not in self.root_images:
                self.root_images.append(image)
            elif image.parent_name and image.parent_name not in self.images:
                # Parent doesn't exist in tree (likely in trash), treat volume as root and put first
                if image not in self.root_images:
                    self.root_images.insert(0, image)
    
    def get_removal_order(self) -> List[OdfImage]:
        """Calculate the order in which images should be removed (children first)"""
        removal_order = []
        visited = set()
        
        def visit_image(image: OdfImage):
            if image.name in visited:
                return
            
            visited.add(image.name)
            
            # Visit children first (depth-first, post-order)
            for child in image.children:
                visit_image(child)
            
            # Add current image after its children
            removal_order.append(image)
        
        # Start with root images
        for root in self.root_images:
            visit_image(root)
        
        return removal_order
    
    def display_tree(self, show_details: bool = True):
        """Display the tree structure"""
        print("\n" + "="*80)
        print("ODF RBD IMAGE HIERARCHY")
        print("="*80)
        
        if not self.root_images:
            print("No images found for the specified LAB GUID")
            return
        
        for root in self.root_images:
            self._display_image(root, "", True, show_details)
        
        print("="*80)
    
    def _display_image(self, image: OdfImage, prefix: str, is_last: bool, show_details: bool):
        """Recursively display an image and its children"""
        current_prefix = "└── " if is_last else "├── "
        status = " [TRASH]" if image.in_trash else ""
        print(f"{prefix}{current_prefix}{image.name}{status}")
        
        if show_details:
            detail_prefix = prefix + ("    " if is_last else "│   ")
            details = [f"Type: {image.image_type.value}"]
            if image.size:
                details.append(f"Size: {self._format_size(image.size)}")
            if image.parent_name:
                details.append(f"Parent: {image.parent_name}")
            if image.internal_snaps:
                snap_status = "protected" if image.is_protected else "unprotected"
                details.append(f"Snaps: {len(image.internal_snaps)} ({snap_status})")
            if image.needs_restoration:
                details.append(f"RESTORE: {image.restoration_reason}")
            if image.needs_flattening:
                details.append("FLATTEN: Required")
            if image.open_by_id:
                details.append(f"UNRESOLVABLE BY NAME: recovered via id={image.open_by_id}")
            
            for detail in details:
                print(f"{detail_prefix}    {detail}")
        
        # Display children
        child_prefix = prefix + ("    " if is_last else "│   ")
        for i, child in enumerate(image.children):
            is_last_child = i == len(image.children) - 1
            self._display_image(child, child_prefix, is_last_child, show_details)
    
    def _format_size(self, size_bytes: int) -> str:
        """Format size in human readable format"""
        size = float(size_bytes)
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size < 1024.0:
                return f"{size:.2f} {unit}"
            size /= 1024.0
        return f"{size:.2f} PB"


class OdfCleaner:
    """Main class for ODF cleanup operations"""
    
    def __init__(self, dry_run: bool = True):
        self.dry_run = dry_run
        self.debug = os.environ.get('DEBUG', 'false').lower() in ['true', '1', 'yes']
        self.ioctx = None
        self.lab_guid = None
        self.pool_name = None
        self.rbd_namespace = None
        self.my_instance_id = None
        self.tree = OdfTree()
        self.removal_stats = {
            'images_removed': 0,
            'csi_snaps_removed': 0,
            'internal_snaps_removed': 0,
            'trash_items_removed': 0,
            'failed_removals': []
        }
        # Cache for dependency analysis (active parent->trash child)
        self._active_to_trash_dependencies = None
        # Track failed trash restorations
        self._failed_trash_restorations = set()
        # Errors during discovery (e.g. a transient rbd.RBD().list() failure) -
        # must never be conflated with "genuinely found nothing to clean up".
        self._discovery_errors: List[str] = []
    
    def _clear_dependency_cache(self):
        """Clear cached dependency analysis"""
        self._active_to_trash_dependencies = None
        self._failed_trash_restorations = set()
        self._discovery_errors = []
    
    def connect(self):
        """Connect to ODF cluster"""
        try:
            self.pool_name = os.environ['CL_POOL']
            conf_file = os.environ['CL_CONF']
            keyring = os.environ['CL_KEYRING']
            self.lab_guid = os.environ['CL_LAB']
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
            self.rbd_namespace = os.environ.get('CL_RBD_NAMESPACE', '')
            if self.rbd_namespace:
                self.ioctx.set_namespace(self.rbd_namespace)
            # Needed to filter our own watch out of watcher checks - opening
            # an image to inspect it registers a watch too.
            self.my_instance_id = self.cluster.get_instance_id()
            
            if self.debug:
                print(f"Connected to ODF cluster: {self.cluster.get_fsid()}")
                print(f"librados version: {self.cluster.version()}")
                print(f"Monitor hosts: {self.cluster.conf_get('mon host')}")
            
            return True
            
        except KeyError as e:
            print(f"Error: Missing environment variable {e}")
            return False
        except Exception as e:
            print(f"Error connecting to cluster: {e}")
            return False
    
    def discover_images(self):
        """Discover all images, csi-snaps, and trash items related to LAB GUID"""
        print(f"\nDiscovering RBD images for LAB GUID: {self.lab_guid}")
        self._clear_dependency_cache()
        
        # Phase 1: Initial GUID-based discovery
        pool_images = self._find_images_by_criteria("pool", guid_check=True, csi_only=False)
        trash_images = self._find_images_by_criteria("trash", guid_check=True, csi_only=False)
        csi_snaps = self._find_images_by_criteria("pool", guid_check=True, csi_only=True)
        
        initial_images = pool_images + trash_images + csi_snaps
        
        # Phase 2: Comprehensive descendant discovery
        additional_images, active_to_trash_deps = self._discover_descendants_and_dependencies(initial_images)
        
        # Phase 3: Dependency analysis and trash csi-snaps
        self._active_to_trash_dependencies = active_to_trash_deps
        trash_csi_snaps = self._find_trash_csi_snaps()
        
        # Combine all discovered images
        all_discovered = initial_images + additional_images + trash_csi_snaps
        
        # Print summary
        print(f"Found: {len(pool_images)} volumes, {len(csi_snaps)} csi-snaps, {len(trash_images)} trash volumes, {len(trash_csi_snaps)} trash csi-snaps")
        if additional_images:
            print(f"  + {len(additional_images)} missing descendants discovered")
        print(f"  Total: {len(all_discovered)} items")
        
        return all_discovered
    
    def _find_images_by_criteria(self, source: str, guid_check: bool = True, csi_only: bool = False) -> List[OdfImage]:
        """Generic method to find images based on criteria"""
        images = []
        try:
            if source == "pool":
                items = rbd.RBD().list(self.ioctx)
                items = [{"name": name, "id": None} for name in items]
            else:  # trash
                items = rbd.RBD().trash_list(self.ioctx)
            
            # Filter by criteria
            filtered_items = []
            for item in items:
                name = item["name"]
                if csi_only and 'csi-snap' not in name:
                    continue
                if not csi_only and 'csi-snap' in name:
                    continue
                if guid_check and self.lab_guid not in name:
                    # For csi-snaps, check parent relationship
                    if 'csi-snap' in name and source == "pool":
                        try:
                            with rbd.Image(self.ioctx, name) as img:
                                parent_info = img.parent_info()
                                if parent_info and self.lab_guid in parent_info[1]:
                                    filtered_items.append(item)
                        except:
                            pass
                    continue
                filtered_items.append(item)
            
            # Create image objects
            for item in filtered_items:
                if source == "pool":
                    image_type = ImageType.CSI_SNAP if 'csi-snap' in item["name"] else ImageType.VOLUME
                    image = self._create_image_from_rbd(item["name"], image_type)
                    if image and 'csi-snap' in item["name"]:
                        try:
                            with rbd.Image(self.ioctx, item["name"]) as img:
                                parent_info = img.parent_info()
                                if parent_info:
                                    image.parent_name = parent_info[1]
                        except:
                            pass
                else:  # trash
                    image_type = ImageType.TRASH_CSI_SNAP if 'csi-snap' in item["name"] else ImageType.TRASH_VOLUME
                    image = self._create_trash_image(item, image_type)
                
                if image:
                    images.append(image)
                    
        except Exception as e:
            print(f"Error finding {source} images: {e}")
            self._discovery_errors.append(f"{source} images (csi_only={csi_only}): {e}")
        
        return images
    
    def _find_trash_csi_snaps(self) -> List[OdfImage]:
        """Find csi-snaps in trash that have active dependencies"""
        csi_snaps = []
        try:
            trash_items = rbd.RBD().trash_list(self.ioctx)
            csi_trash = [item for item in trash_items if 'csi-snap' in item['name']]
            
            print(f"  Found {len(csi_trash)} csi-snaps in trash, using cached dependency analysis...")
            
            # Use cached dependency analysis
            active_dependencies = self._active_to_trash_dependencies or {}
            
            for item in csi_trash:
                if self._is_trash_item_referenced(item, active_dependencies):
                    image = self._create_trash_image(item, ImageType.TRASH_CSI_SNAP)
                    if image:
                        csi_snaps.append(image)
                        print(f"    Included trash csi-snap: {item['name']} (referenced by active images)")
                else:
                    print(f"    Skipped trash csi-snap: {item['name']} (no active dependencies)")
                    
        except Exception as e:
            print(f"Error finding trash csi-snaps: {e}")
            self._discovery_errors.append(f"trash csi-snaps: {e}")
        
        return csi_snaps
    
    def _direct_children_trash_safe(self, img) -> Tuple[List[Dict], bool]:
        """One level of children via set_snap()/set_snap_by_id() per snapshot +
        list_children2() - works even when a snapshot is in RBD's trash
        namespace, which makes list_descendants() fail outright (confirmed
        against real cluster output: a trashed snap can still have a live
        child, which is exactly why it's still sitting there). The caller's
        own BFS loop handles recursion, so this only needs one level.
        Returns (children, ok) - ok=False means at least one snapshot's
        children couldn't be listed (confirmed this can happen even via
        set_snap_by_id) - that's "unknown", never "confirmed no children"."""
        children = []
        ok = True
        for snap in img.list_snaps():
            try:
                if 'trash' in snap:
                    img.set_snap_by_id(snap['id'])
                else:
                    img.set_snap(snap['name'])
            except Exception:
                ok = False
                continue
            try:
                children.extend(img.list_children2())
            except AttributeError:
                try:
                    children.extend({'image': c[1], 'trash': False} for c in img.list_children())
                except Exception:
                    ok = False
            except Exception:
                ok = False
            finally:
                try:
                    img.set_snap(None)
                except Exception:
                    pass
        return children, ok

    def _discover_descendants_and_dependencies(self, discovered_images: List[OdfImage]) -> Tuple[List[OdfImage], Dict[str, List[str]]]:
        """Recursively scan for missing descendants and track trash dependencies"""
        all_additional = []
        active_to_trash_deps = {}
        discovered_names = {img.name for img in discovered_images}
        image_lookup: Dict[str, OdfImage] = {img.name: img for img in discovered_images}
        
        # Start with originally discovered active images
        images_to_scan = [img for img in discovered_images if not img.in_trash]
        scanned_names = set()  # Track what we've already scanned to avoid loops
        
        while images_to_scan:
            current_batch = []
            
            # Scan current batch of images
            for image in images_to_scan:
                # Skip if already scanned this image
                if image.name in scanned_names:
                    continue
                    
                scanned_names.add(image.name)
                
                try:
                    with rbd.Image(self.ioctx, image.name) as img:
                        try:
                            descendants = list(img.list_descendants())
                        except Exception:
                            descendants, ok = self._direct_children_trash_safe(img)
                            if not ok:
                                raise RuntimeError("could not fully resolve descendants (trash-safe fallback incomplete)")
                        
                        for desc in descendants:
                            if isinstance(desc, dict):
                                desc_name = desc.get('name') or desc.get('image') or desc.get('child') or str(desc)
                            else:
                                desc_name = str(desc)
                            if not desc_name:
                                continue
                            
                            # Handle trash descendants - track dependency and update parent
                            if desc.get('trash', False):
                                if image.name not in active_to_trash_deps:
                                    active_to_trash_deps[image.name] = []
                                active_to_trash_deps[image.name].append(desc_name)
                                
                                # Also update parent relationship for trash item (only if no parent set)
                                existing_img = image_lookup.get(desc_name)
                                if existing_img and not existing_img.parent_name:
                                    existing_img.parent_name = image.name
                                    print(f"    Updated trash parent: {desc_name} -> {image.name}")
                                continue
                            
                            # Handle active descendants - add to discovery
                            if desc_name not in discovered_names:
                                desc_pool = desc.get('pool') if isinstance(desc, dict) else None
                                if desc_pool and desc_pool != self.pool_name:
                                    print(f"    [!] CROSS-POOL DESCENDANT: {desc_name} lives in pool "
                                          f"'{desc_pool}', not '{self.pool_name}'")
                                    continue
                                # Determine image type based on name
                                desc_image_type = ImageType.CSI_SNAP if 'csi-snap' in desc_name else ImageType.VOLUME
                                new_image = self._create_image_from_rbd(desc_name, desc_image_type)
                                if new_image:
                                    new_image.parent_name = image.name
                                    # RBD clone lineage from our own volume is proof of
                                    # ownership - CSI-generated names never carry the GUID
                                    # regardless of which lab they belong to, so no name
                                    # check is needed here. Recurse and delete normally.
                                    current_batch.append(new_image)
                                    all_additional.append(new_image)
                                    discovered_names.add(desc_name)
                                    image_lookup[desc_name] = new_image
                            else:
                                # Handle already-discovered descendant - update parent relationship (only if no parent set)
                                existing_img = image_lookup.get(desc_name)
                                if existing_img and not existing_img.parent_name:
                                    existing_img.parent_name = image.name
                                    print(f"    Updated parent: {desc_name} -> {image.name}")
                                    
                except Exception as e:
                    if self.debug:
                        print(f"    DEBUG: Error scanning descendants of {image.name}: {e}")
                    continue
            
            # Prepare next batch (newly discovered active images)
            images_to_scan = current_batch
            if current_batch:
                print(f"    Found {len(current_batch)} new images to scan for descendants...")
        
        if all_additional:
            print(f"    Recursive scan complete: found {len(all_additional)} total missing descendants")
        if active_to_trash_deps:
            dep_count = sum(len(deps) for deps in active_to_trash_deps.values())
            print(f"    Found {dep_count} active->trash dependencies")
        
        return all_additional, active_to_trash_deps
    
    def _is_trash_item_referenced(self, trash_item: dict, active_dependencies: Dict[str, List[str]]) -> bool:
        """Check if a trash item is referenced by any active LAB images"""
        trash_name = trash_item['name']
        
        # Check if this trash item appears in any dependency list
        for active_image, trash_parents in active_dependencies.items():
            if trash_name in trash_parents:
                print(f"      Trash item {trash_name} is referenced by active image {active_image}")
                return True
                
        return False
    
    def _build_odf_image(self, img, img_name: str, image_type: ImageType) -> OdfImage:
        """Construct an OdfImage from an already-open rbd.Image handle."""
        stat = img.stat()
        creation_time = None
        try:
            ts = img.create_timestamp()
            if isinstance(ts, datetime):
                creation_time = str(ts)
            elif ts:
                creation_time = str(datetime.fromtimestamp(ts))
            else:
                creation_time = "Unknown"
        except Exception:
            creation_time = "Unknown"

        parent_name = None
        try:
            parent_info = img.parent_info()
            if parent_info:
                parent_name = parent_info[1]
        except:
            pass

        internal_snaps = [snap['name'] for snap in img.list_snaps()]

        is_protected = False
        try:
            for snap in img.list_snaps():
                try:
                    if img.is_protected_snap(snap['name']):
                        is_protected = True
                        break
                except Exception as snap_err:
                    # Only show warning for unexpected errors, not "image not found"
                    if "RBD image not found" not in str(snap_err):
                        print(f"    Warning: Could not check protection for snapshot {snap['name']}: {snap_err}")
        except Exception as snap_list_err:
            print(f"    Warning: Could not list snapshots for {img_name}: {snap_list_err}")

        image = OdfImage(
            name=img_name,
            image_type=image_type,
            size=stat['size'],
            creation_time=creation_time,
            parent_name=parent_name,
            is_protected=is_protected
        )
        image.internal_snaps = internal_snaps
        return image

    def _resolve_via_list2(self, image_name: str) -> Optional[str]:
        """rbd_id.<name> and rbd_directory's name_<name> entry can both be
        missing while the image is still real - rbd.RBD().list2() sometimes
        resolves what direct omap lookups can't"""
        try:
            for item in rbd.RBD().list2(self.ioctx):
                if item.get('name') == image_name:
                    return item.get('id')
        except Exception:
            pass
        return None

    def _create_image_from_rbd(self, img_name: str, image_type: ImageType) -> Optional[OdfImage]:
        """Create an OdfImage from an RBD image"""
        try:
            with rbd.Image(self.ioctx, img_name) as img:
                return self._build_odf_image(img, img_name, image_type)

        except Exception as e:
            phantom_id = self._check_phantom_entry(img_name)
            if phantom_id:
                print(f"  PHANTOM ENTRY detected: {img_name} (rbd_id -> id={phantom_id}, but rbd_header.{phantom_id} is missing)")
                image = OdfImage(name=img_name, image_type=image_type)
                image.phantom_image_id = phantom_id
                return image

            recovered_id = self._resolve_via_list2(img_name)
            if recovered_id:
                try:
                    with rbd.Image(self.ioctx, image_id=recovered_id) as img:
                        print(f"  RECOVERED via id lookup: {img_name} is unresolvable by name "
                              f"(rbd_id/rbd_directory both missing) but opened fine by id={recovered_id}")
                        image = self._build_odf_image(img, img_name, image_type)
                        image.open_by_id = recovered_id
                        return image
                except Exception as e2:
                    print(f"Error opening recovered id {recovered_id} for {img_name}: {e2}")

            print(f"Error creating image for {img_name}: {e}")
            self._discovery_errors.append(f"image {img_name}: {e}")
            return None

    def _check_phantom_entry(self, image_name: str) -> Optional[str]:
        """Detect a known corruption pattern: rbd_id.<name> exists and resolves
        to an internal id, but that id's rbd_header object is missing."""
        id_obj = f"rbd_id.{image_name}"
        try:
            self.ioctx.stat(id_obj)
        except Exception:
            return None  # no id pointer either
        try:
            raw = self.ioctx.read(id_obj, length=4096)
            length = struct.unpack('<I', raw[:4])[0]
            image_id = raw[4:4 + length].decode('utf-8', errors='replace')
        except Exception:
            return None
        try:
            self.ioctx.stat(f"rbd_header.{image_id}")
            return None  # header exists
        except Exception:
            return image_id

    def _execute_phantom_cleanup(self, image_name: str, image_id: str) -> bool:
        """Remove a confirmed phantom rbd_id pointer and omap keys.
        Never touches rbd_header (already gone)."""
        id_obj = f"rbd_id.{image_name}"
        try:
            write_op = self.ioctx.create_write_op()
            self.ioctx.remove_omap_keys(write_op, (f"name_{image_name}", f"id_{image_id}"))
            self.ioctx.operate_write_op(write_op, "rbd_directory")
            write_op.release()
            self.ioctx.remove_object(id_obj)
            print(f"    Successfully deleted: {image_name} (phantom entry, id={image_id})")
            return True
        except Exception as e:
            print(f"    ERROR: Failed to clean up phantom entry {image_name}: {e}")
            return False

    def _create_trash_image(self, trash_item: dict, image_type: ImageType) -> Optional[OdfImage]:
        """Create an OdfImage from a trash item"""
        try:
            # Handle deferment_end_time - could be timestamp or datetime object
            creation_time = None
            defer_time = trash_item.get('deferment_end_time', 0)
            if defer_time:
                if isinstance(defer_time, datetime):
                    creation_time = str(defer_time)
                else:
                    creation_time = str(datetime.fromtimestamp(defer_time))
            
            image = OdfImage(
                name=trash_item['name'],
                image_type=image_type,
                in_trash=True,
                trash_id=trash_item['id'],
                creation_time=creation_time
            )
            return image
            
        except Exception as e:
            print(f"Error creating trash image for {trash_item['name']}: {e}")
            return None
    
    def build_tree(self, discovered_items: List[OdfImage], debug: bool = False):
        """Build the hierarchical tree from discovered items"""
        print(f"\nBuilding hierarchical tree...")
        
        if debug:
            # Debug: Show discovered items
            print("Discovered items:")
            for item in discovered_items:
                parent_info = f" (parent: {item.parent_name})" if item.parent_name else " (no parent)"
                print(f"  - {item.name} [{item.image_type.value}]{parent_info}")
        
        # Add all images to tree
        for image in discovered_items:
            self.tree.add_image(image)
        
        # Build relationships
        self.tree.build_relationships()
        
        if debug:
            # Debug: Show what ended up in the tree
            print(f"Tree contents:")
            print(f"  All images: {list(self.tree.images.keys())}")
            print(f"  Root images: {[img.name for img in self.tree.root_images]}")
        
        print(f"Tree built with {len(self.tree.images)} images and {len(self.tree.root_images)} root images")
    
    def plan_removal(self) -> List[OdfImage]:
        """Plan the removal order"""
        print(f"\nPlanning removal order...")
        removal_order = self.tree.get_removal_order()
        
        print("Planned removal order:")
        for i, image in enumerate(removal_order, 1):
            status = "TRASH" if image.in_trash else "ACTIVE"
            print(f"  {i:2d}. {image.name} ({image.image_type.value}) [{status}]")
        
        return removal_order
    
    def execute_cleanup(self, removal_order: List[OdfImage]):
        """Execute the cleanup process"""
        if self.dry_run:
            print(f"\n{'='*80}")
            print("DRY RUN MODE - NO ACTUAL DELETION WILL OCCUR")
            print("="*80)
            
            print(f"\nDry run cleanup simulation for {len(removal_order)} items...")
            
            for i, image in enumerate(removal_order, 1):
                print(f"\n[{i}/{len(removal_order)}] Processing: {image.name}")
                if image.phantom_image_id:
                    print(f"  DRY RUN: Would clean up phantom entry (dangling rbd_id pointer, id={image.phantom_image_id})")
                    continue
                print(f"  DRY RUN: Would remove {image.image_type.value}")
                if image.internal_snaps:
                    print(f"  DRY RUN: Would remove {len(image.internal_snaps)} internal snapshots")
                if image.in_trash:
                    print(f"  DRY RUN: Would restore from trash first")
                if image.needs_flattening:
                    print(f"  DRY RUN: Would flatten to remove dependencies")
        else:
            print(f"\n{'='*80}")
            print("LIVE MODE - ACTUAL DELETION WILL OCCUR")
            print("="*80)
            
            # Check for multi-phase operations
            if self._active_to_trash_dependencies:
                print(f"\nWARNING: Multi-phase operations detected!")
                print(f"Some images will be restored, flattened, then deleted.")
                print(f"This process may take additional time.")
            
            print(f"\nAbout to delete {len(removal_order)} RBD images for LAB GUID: {self.lab_guid}")
            print(f"Pool: {self.pool_name}")
            
            # Execute initial cleanup attempt
            initial_failed_count = self._execute_removal_batch(removal_order, "Initial cleanup")
            
            # If we had failures, try trash purge and retry
            if initial_failed_count > 0:
                print(f"\nRETRY STRATEGY - {initial_failed_count} FAILURES DETECTED")
                print("Attempting trash purge to clear blocking items...")
                
                # Get the failed items from the last attempt
                failed_items = [item for item in removal_order 
                              if item.name in self.removal_stats['failed_removals']]
                
                # Attempt trash purge (non-fatal if it fails)
                purge_success = self._purge_expired_trash()
                
                # After purge, check which failed items are actually still present
                print("Checking which failed items still exist after purge...")
                still_failed_items = []
                items_cleaned_by_purge = []
                
                for item in failed_items:
                    if self._item_still_exists(item):
                        still_failed_items.append(item)
                    else:
                        items_cleaned_by_purge.append(item)
                        # Update removal stats for items cleaned by purge
                        if item.image_type == ImageType.TRASH_VOLUME:
                            self.removal_stats['trash_items_removed'] += 1
                        elif item.image_type == ImageType.CSI_SNAP:
                            self.removal_stats['csi_snaps_removed'] += 1
                        elif item.image_type == ImageType.VOLUME:
                            self.removal_stats['images_removed'] += 1
                        # Remove from failed_removals list since it's now cleaned up
                        if item.name in self.removal_stats['failed_removals']:
                            self.removal_stats['failed_removals'].remove(item.name)
                
                if items_cleaned_by_purge:
                    print(f"Trash purge cleaned up {len(items_cleaned_by_purge)} items:")
                    for item in items_cleaned_by_purge:
                        print(f"  - {item.name} ({item.image_type.value})")
                
                if still_failed_items:
                    print(f"Retrying {len(still_failed_items)} items that still exist...")
                    # Clear failed removals for items we're about to retry
                    for item in still_failed_items:
                        if item.name in self.removal_stats['failed_removals']:
                            self.removal_stats['failed_removals'].remove(item.name)
                    
                    retry_failed_count = self._execute_removal_batch(still_failed_items, "Post-purge retry")
                    
                    if retry_failed_count == 0:
                        print("All remaining failed items successfully removed after trash purge!")
                    else:
                        print(f"Warning: {retry_failed_count} items still failed after trash purge and retry")
                else:
                    print("All failed items were cleaned up by trash purge!")
                    retry_failed_count = 0
                    
        final_failure_count = len(self.removal_stats['failed_removals'])
        restoration_failure_count = len(self._failed_trash_restorations)
        
        if final_failure_count == 0 and restoration_failure_count == 0:
            self._final_verification()
            self._generate_report()
    
    def _execute_removal_batch(self, items: List[OdfImage], batch_name: str) -> int:
        """Execute removal for a batch of items and return count of failures"""
        print(f"\n{batch_name} for {len(items)} items...")
        
        initial_failure_count = len(self.removal_stats['failed_removals'])
        
        for i, image in enumerate(items, 1):
            print(f"\n[{i}/{len(items)}] Processing: {image.name}")
            
            # Mark images that need flattening based on dependencies
            if self._needs_flattening_for_dependencies(image):
                image.needs_flattening = True
                image.restoration_reason = "Remove dependencies before deletion"
            
            # Attempt removal
            success = self._remove_image(image)
            if success:
                self._update_removal_stats(image)
                print(f"  SUCCESS: Removed {image.name}")
            else:
                # Only add to failed_removals if not already there
                if image.name not in self.removal_stats['failed_removals']:
                    self.removal_stats['failed_removals'].append(image.name)
                print(f"  FAILED: Could not remove {image.name}")
            
            # Brief pause between operations
            time.sleep(3)
        
        current_failure_count = len(self.removal_stats['failed_removals'])
        batch_failures = current_failure_count - initial_failure_count
        
        return batch_failures
    
    def _purge_expired_trash(self) -> bool:
        """Purge expired trash items to prevent blocking cleanup operations"""
        print(f"\nPurging expired trash items from pool '{self.pool_name}'...")
        
        try:
            # Execute trash purge
            print("  Executing trash purge...")
            rbd.RBD().trash_purge(self.ioctx, 0)
            print("  Trash purge completed")
            time.sleep(10)
            return True
            
        except Exception as e:
            print(f"  WARNING: Trash purge failed: {e}")
            print("  Cannot retry failed items")
            return False

    def _item_still_exists(self, item: OdfImage) -> bool:
        """Check if an OdfImage item still exists in the cluster"""
        try:
            if item.image_type == ImageType.TRASH_VOLUME:
                # Check if item still exists in trash
                trash_list = list(rbd.RBD().trash_list(self.ioctx))
                return any(trash_item['name'] == item.name for trash_item in trash_list)
            else:
                # Check if item still exists in active pool (volumes and csi-snaps)
                active_images = rbd.RBD().list(self.ioctx)
                return item.name in active_images
        except Exception as e:
            if self.debug:
                print(f"  Warning: Error checking existence of {item.name}: {e}")
            # If we can't check, assume it still exists to be safe
            return True
    
    def _needs_flattening_for_dependencies(self, image: OdfImage) -> bool:
        """Check if image needs flattening based on dependency analysis"""
        if not self._active_to_trash_dependencies:
            return False
        
        # Check if this image is mentioned in dependency analysis
        for active_image, trash_parents in self._active_to_trash_dependencies.items():
            if image.name == active_image:
                return True  # This active image depends on trash items
            if image.name in trash_parents:
                return False  # This is a trash item that will be restored
        
        return False
    
    def _needs_fallback_flattening(self, image: OdfImage) -> bool:
        """Check if image needs flattening due to failed trash restorations"""
        if not self._active_to_trash_dependencies or not self._failed_trash_restorations:
            return False
        
        # Check if this active image depends on any failed trash restorations
        for active_image, trash_parents in self._active_to_trash_dependencies.items():
            if image.name == active_image:
                # Check if any of its trash parents failed to restore
                failed_parents = set(trash_parents) & self._failed_trash_restorations
                if failed_parents:
                    print(f"    Fallback flattening needed: depends on failed trash items {failed_parents}")
                    return True
        
        return False
    
    def _remove_image(self, image: OdfImage) -> bool:
        """Remove a single RBD image with proper handling"""
        print(f"  Removing {image.image_type.value}: {image.name}")

        if image.phantom_image_id:
            print(f"    Strategy: phantom entry cleanup (dangling rbd_id, missing rbd_header)")
            return self._execute_phantom_cleanup(image.name, image.phantom_image_id)

        try:
            needs_flatten = image.needs_flattening or self._needs_fallback_flattening(image)
            strategy = []
            if image.open_by_id:
                strategy.append(f"open by id={image.open_by_id} (rbd_id/rbd_directory missing)")
            if image.in_trash:
                strategy.append("restore from trash")
            if needs_flatten:
                strategy.append("flatten")
            strategy.append("remove")
            print(f"    Strategy: {' -> '.join(strategy)}")

            # Handle trash items first - restore them temporarily
            if image.in_trash:
                if not self._restore_from_trash(image):
                    # Failed to restore - skip this trash item but don't fail overall cleanup
                    print(f"  SKIPPED: Could not restore {image.name}, leaving in trash")
                    self._failed_trash_restorations.add(image.name)
                    return True  # Consider this "successful" to continue cleanup
                # After restoration, treat as active image for deletion
            
            # Handle multi-phase operations or fallback flattening
            if needs_flatten:
                if not self._flatten_image(image):
                    return False
            
            # Remove the active image
            return self._remove_active_image(image)
            
        except Exception as e:
            print(f"  ERROR: Failed to remove {image.name}: {e}")
            return False
    
    def _restore_from_trash(self, image: OdfImage) -> bool:
        """Restore an image from trash temporarily for deletion"""
        print(f"    Restoring from trash: {image.name} (ID: {image.trash_id})")
        
        try:
            # Restore image from trash
            rbd.RBD().trash_restore(self.ioctx, image.trash_id, image.name)
            print(f"    Successfully restored: {image.name}")
            return True
            
        except Exception as e:
            print(f"    ERROR: Failed to restore {image.name}: {e}")
            print(f"    This trash item will be skipped, but dependent active images will be flattened")
            return False
    
    def _flatten_image(self, image: OdfImage) -> bool:
        """Flatten an image to remove parent dependencies"""
        print(f"    Flattening image: {image.name}")
        
        try:
            with rbd.Image(self.ioctx, image.name) as img:
                # Check if image actually needs flattening
                try:
                    parent_info = img.parent_info()
                    if not parent_info:
                        print(f"    Image {image.name} has no parent, skipping flatten")
                        return True
                except:
                    # No parent, nothing to flatten
                    print(f"    Image {image.name} has no parent, skipping flatten")
                    return True
                
                # Perform flattening
                img.flatten()
                print(f"    Flattening initiated for: {image.name}")
                
                # Wait for flatten to complete
                self._wait_for_flatten_completion(img, image.name)
                print(f"    Successfully flattened: {image.name}")
                return True
                
        except Exception as e:
            print(f"    ERROR: Failed to flatten {image.name}: {e}")
            return False
    
    def _wait_for_flatten_completion(self, img, img_name: str, max_wait: int = 300):
        """Wait for flatten operation to complete"""
        print(f"    Waiting for flatten completion...")
        
        start_time = time.time()
        while time.time() - start_time < max_wait:
            try:
                # Check if still has parent
                parent_info = img.parent_info()
                if not parent_info:
                    print(f"    Flatten completed for: {img_name}")
                    return True
            except:
                # No parent info means flatten completed
                print(f"    Flatten completed for: {img_name}")
                return True
            
            print(f"    Still flattening... ({int(time.time() - start_time)}s)")
            time.sleep(10)
        
        print(f"    WARNING: Flatten may still be in progress after {max_wait}s")
        return True  # Continue anyway
    
    # Name varies by packaging: docs say list_watchers(), RHEL9's
    # python3-rbd 18.2.8 exposes watchers_list() instead. Try both.
    WATCHER_METHOD_NAMES = ('list_watchers', 'watchers_list')

    def _get_watchers(self, img, image_name: str) -> List[str]:
        """List external watchers on an open image (excludes our own watch,
        which opening the image to inspect it registers)."""
        for method_name in self.WATCHER_METHOD_NAMES:
            method = getattr(img, method_name, None)
            if method is None:
                continue
            raw_watchers = list(method())
            external = [
                w for w in raw_watchers
                if not (isinstance(w, dict) and w.get('id') == self.my_instance_id)
            ]
            return [str(w) for w in external]
        raise RuntimeError(f"none of {self.WATCHER_METHOD_NAMES} found on rbd.Image "
                            f"for {image_name} - unknown Ceph client binding")

    def _open_by_ref(self, image: OdfImage):
        """Open by name, or by id if OdfImage.open_by_id was set during
        discovery (name resolution known broken for this one)."""
        if image.open_by_id:
            return rbd.Image(self.ioctx, image_id=image.open_by_id)
        return rbd.Image(self.ioctx, image.name)

    def _remove_active_image(self, image: OdfImage) -> bool:
        """Remove an active RBD image (volumes, csi-snaps)"""
        try:
            with self._open_by_ref(image) as img:
                # Watcher failsafe: list_descendants() only catches RBD clone
                # children. Refuse to delete anything that's currently watched.
                try:
                    watchers = self._get_watchers(img, image.name)
                except Exception as e:
                    print(f"    ERROR: Could not check watchers for {image.name}: {e} - refusing to delete")
                    return False
                if watchers:
                    print(f"    ERROR: Image {image.name} has {len(watchers)} active watcher(s) - refusing to delete")
                    print(f"    Watchers: {watchers}")
                    return False

                # Get current state
                descendants = list(img.list_descendants())
                active_descendants = [d for d in descendants if not d.get('trash', False)]
                
                if active_descendants:
                    print(f"    ERROR: Image {image.name} still has {len(active_descendants)} active descendants")
                    # Try multiple ways to extract descendant names
                    desc_names = []
                    for d in active_descendants:
                        if isinstance(d, dict):
                            name = d.get('name') or d.get('image') or d.get('child') or str(d)
                        else:
                            name = str(d)
                        desc_names.append(name)
                    print(f"    Descendants: {desc_names}")
                    print(f"    Raw descendant data: {active_descendants}")
                    for d in active_descendants:
                        d_pool = d.get('pool') if isinstance(d, dict) else None
                        if d_pool and d_pool != self.pool_name:
                            print(f"    [!] CROSS-POOL DESCENDANT: {d.get('image', d)} lives in pool "
                                  f"'{d_pool}', not '{self.pool_name}'")
                    return False
                
                # Remove internal snapshots first
                if not self._remove_internal_snapshots(img, image.name):
                    return False
                
                # Try to flatten if needed (safety check)
                try:
                    img.flatten()
                    print(f"    Final flatten for: {image.name}")
                    time.sleep(5)  # Brief wait
                except:
                    pass  # Already flat or no parent
            
            # Remove the image itself
            print(f"    Deleting image: {image.name}")
            rbd.RBD().remove(self.ioctx, image.name)
            print(f"    Successfully deleted: {image.name}")
            return True
            
        except Exception as e:
            print(f"    ERROR: Failed to delete {image.name}: {e}")
            return False
    
    def _remove_internal_snapshots(self, img, img_name: str) -> bool:
        """Remove all internal snapshots from an image"""
        try:
            snapshots = list(img.list_snaps())
            if not snapshots:
                print(f"    No internal snapshots to remove")
                return True
            
            print(f"    Removing {len(snapshots)} internal snapshots...")
            
            for snap in snapshots:
                snap_name = snap['name']
                print(f"      Removing snapshot: {snap_name}")
                
                try:
                    # RBD "clone v2" moves a deleted snapshot with live clones
                    # into a trash namespace instead of blocking the delete
                    if 'trash' in snap:
                        img.remove_snap_by_id(snap['id'])
                        print(f"        Successfully removed trashed snapshot: {snap_name}")
                        time.sleep(2)
                        continue

                    # Unprotect if protected
                    if img.is_protected_snap(snap_name):
                        print(f"        Unprotecting snapshot: {snap_name}")
                        img.unprotect_snap(snap_name)
                    
                    # Remove snapshot
                    img.remove_snap(snap_name)
                    print(f"        Successfully removed snapshot: {snap_name}")
                    time.sleep(2)  # Brief pause between snapshots
                    
                except Exception as snap_err:
                    print(f"        ERROR: Failed to remove snapshot {snap_name}: {snap_err}")
                    return False
            
            print(f"    All internal snapshots removed from: {img_name}")
            return True
            
        except Exception as e:
            print(f"    ERROR: Failed to process snapshots for {img_name}: {e}")
            return False
    
    def _update_removal_stats(self, image: OdfImage):
        """Update removal statistics"""
        if image.image_type == ImageType.VOLUME:
            self.removal_stats['images_removed'] += 1
        elif image.image_type in [ImageType.CSI_SNAP, ImageType.TRASH_CSI_SNAP]:
            self.removal_stats['csi_snaps_removed'] += 1
        elif image.image_type == ImageType.TRASH_VOLUME:
            self.removal_stats['trash_items_removed'] += 1
        
        self.removal_stats['internal_snaps_removed'] += len(image.internal_snaps)
    
    def _generate_report(self):
        """Generate cleanup report"""
        print(f"\n{'='*80}")
        print("CLEANUP REPORT")
        print("="*80)
        print(f"LAB GUID: {self.lab_guid}")
        print(f"Pool: {self.pool_name}")
        print(f"Dry Run: {'YES' if self.dry_run else 'NO'}")
        print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("-" * 80)
        print(f"Images removed: {self.removal_stats['images_removed']}")
        print(f"CSI-snaps removed: {self.removal_stats['csi_snaps_removed']}")
        print(f"Internal snaps removed: {self.removal_stats['internal_snaps_removed']}")
        print(f"Trash items removed: {self.removal_stats['trash_items_removed']}")
        print(f"Failed removals: {len(self.removal_stats['failed_removals'])}")
        print(f"Failed trash restorations: {len(self._failed_trash_restorations)}")
        
        if self.removal_stats['failed_removals']:
            print("\nFailed removals:")
            for item in self.removal_stats['failed_removals']:
                print(f"  - {item}")
        
        if self._failed_trash_restorations:
            print("\nFailed trash restorations (left in trash):")
            for item in self._failed_trash_restorations:
                print(f"  - {item}")
        
        print("="*80)
    
    def _final_verification(self):
        """Final verification that no objects with the GUID remain in the pool"""
        print("FINAL VERIFICATION - Checking for remaining objects...")
        
        if self.dry_run:
            print("  DRY RUN: Would verify no objects remain with GUID")
            return
        
        try:
            remaining_objects = []
            
            # Check active pool images
            all_rbd_images = rbd.RBD().list(self.ioctx)
            remaining_active = [img for img in all_rbd_images if self.lab_guid in img]
            if remaining_active:
                remaining_objects.extend([f"ACTIVE: {img}" for img in remaining_active])
            
            # Check trash items
            trash_items = list(rbd.RBD().trash_list(self.ioctx))
            remaining_trash = [item['name'] for item in trash_items if self.lab_guid in item['name']]
            if remaining_trash:
                remaining_objects.extend([f"TRASH: {item}" for item in remaining_trash])
            
            # Report results and handle remaining objects
            if remaining_objects:
                print(f"  WARNING: Found {len(remaining_objects)} remaining objects with GUID:")
                for obj in remaining_objects:
                    print(f"    - {obj}")
                
                print("  Attempting final cleanup of remaining objects...")
                
                # Create OdfImage objects for remaining items and attempt cleanup
                final_cleanup_items = []
                
                # Process remaining active images
                for img_name in remaining_active:
                    try:
                        # Determine if it's a CSI snap or regular volume
                        image_type = ImageType.CSI_SNAP if 'csi-snap' in img_name else ImageType.VOLUME
                        image = self._create_image_from_rbd(img_name, image_type)
                        if image:
                            final_cleanup_items.append(image)
                    except Exception as e:
                        print(f"    Warning: Could not process {img_name}: {e}")
                
                # Process remaining trash items
                for item_name in remaining_trash:
                    try:
                        # Find the trash item details
                        trash_item = next((item for item in trash_items if item['name'] == item_name), None)
                        if trash_item:
                            # Determine if it's a CSI snap or regular volume in trash
                            image_type = ImageType.TRASH_CSI_SNAP if 'csi-snap' in item_name else ImageType.TRASH_VOLUME
                            image = self._create_trash_image(trash_item, image_type)
                            if image:
                                final_cleanup_items.append(image)
                    except Exception as e:
                        print(f"    Warning: Could not process trash item {item_name}: {e}")
                
                if final_cleanup_items:
                    print(f"  Attempting cleanup of {len(final_cleanup_items)} remaining items...")
                    
                    # Clear any previous failed removals for final attempt
                    self.removal_stats['failed_removals'] = []
                    
                    # Attempt final cleanup
                    final_failed_count = self._execute_removal_batch(final_cleanup_items, "Final verification cleanup")
                    
                    if final_failed_count == 0:
                        print("  SUCCESS: All remaining objects successfully cleaned up!")
                        print(f"  Cleanup completed successfully for LAB GUID: {self.lab_guid}")
                    else:
                        print(f"  WARNING: {final_failed_count} objects still remain after final cleanup attempt")
                        print("  These objects may need manual investigation")
                else:
                    print("  Could not create cleanup objects for remaining items")
            else:
                print("  SUCCESS: No objects with GUID found in pool")
                print(f"  Cleanup completed successfully for LAB GUID: {self.lab_guid}")
                
        except Exception as e:
            print(f"  ERROR: Could not perform final verification: {e}")
            print("  Continuing with cleanup report...")
    
    def _remove_namespace_if_empty(self):
        """If CL_RBD_NAMESPACE was set, remove it once confirmed empty of images/trash.
        Best-effort, never affects overall cleanup success."""
        ns = self.rbd_namespace
        if not ns:
            return
        try:
            remaining_images = list(rbd.RBD().list(self.ioctx))
            remaining_trash = list(rbd.RBD().trash_list(self.ioctx))
        except Exception as e:
            print(f"[x] Could not check whether RBD namespace '{ns}' is empty: {e}")
            return

        if remaining_images or remaining_trash:
            print(f"  RBD namespace '{ns}' still has {len(remaining_images)} image(s)/"
                  f"{len(remaining_trash)} trash item(s) - leaving it in place")
            return

        if self.dry_run:
            print(f"  RBD namespace '{ns}' is now empty - would remove it "
                  f"(rbd namespace rm {self.pool_name}/{ns})")
            return

        # namespace_remove operates at the pool level, not inside the
        # namespace itself - the ioctx needs to be back in the default
        # namespace for the call to find it.
        self.ioctx.set_namespace('')
        try:
            rbd.RBD().namespace_remove(self.ioctx, ns)
            print(f"  [v] Removed empty RBD namespace: {ns}")
        except Exception as e:
            print(f"  [x] Failed to remove RBD namespace '{ns}': {e}")

    def cleanup(self):
        """Main cleanup orchestration"""
        if not self.connect():
            return False
        
        try:
            # Discovery phase
            discovered_items = self.discover_images()
            if not discovered_items:
                if self._discovery_errors:
                    # Errored, not empty - don't report false success.
                    print(f"ERROR: Discovery failed for GUID {self.lab_guid} - "
                          f"cannot confirm there is nothing to clean up:")
                    for err in self._discovery_errors:
                        print(f"  - {err}")
                    return False
                print("No items found for cleanup")
                self._remove_namespace_if_empty()
                return True
            
            # Tree building phase
            debug_mode = self.dry_run or self.debug
            self.build_tree(discovered_items, debug=debug_mode)
            
            # Display tree
            self.tree.display_tree()
            
            # Planning phase
            removal_order = self.plan_removal()
            
            # Execution phase
            self.execute_cleanup(removal_order)
            
            # Check if there were any failures
            failed_count = len(self.removal_stats['failed_removals'])
            if failed_count > 0:
                print(f"ERROR: Cleanup failed for {failed_count} items")
                return False
            
            self._remove_namespace_if_empty()
            return True
            
        except Exception as e:
            print(f"Error during cleanup: {e}")
            return False
        finally:
            if self.ioctx:
                self.ioctx.close()


def main():
    """Main entry point"""
    print("ODF Cleanup")
    print("=" * 80)
    
    # Check environment variables
    required_envs = ['CL_LAB', 'CL_POOL', 'CL_CONF', 'CL_KEYRING']
    missing_envs = [env for env in required_envs if env not in os.environ]
    
    if missing_envs:
        print(f"Error: Missing environment variables: {', '.join(missing_envs)}")
        print("\nRequired environment variables:")
        for env in required_envs:
            print(f"  {env}")
        print("\nOptional environment variables:")
        print("  DRY_RUN=[true/false]     - Enable dry-run mode (default: true)")
        print("  DEBUG=[true/false]       - Enable debug output (default: false)")
        print("  CL_RBD_NAMESPACE         - RBD namespace within the pool (Ceph multi-tenancy, distinct")
        print("    from k8s namespaces) some provisioners isolate a lab's images into (default: pool's default namespace)")
        return 1
    
    # Check for dry run mode
    dry_run = os.environ.get('DRY_RUN', 'true').lower() in ['true', '1', 'yes']
    
    # Show current configuration
    print(f"Configuration:")
    print(f"  LAB GUID: {os.environ['CL_LAB']}")
    print(f"  Pool: {os.environ['CL_POOL']}")
    print(f"  Dry Run: {'YES' if dry_run else 'NO'}")
    print(f"  Debug: {os.environ.get('DEBUG', 'false').upper()}")
    rbd_namespace = os.environ.get('CL_RBD_NAMESPACE', '')
    if rbd_namespace:
        print(f"  RBD Namespace: {rbd_namespace}")
    
    if not dry_run:
        print(f"\nWARNING: LIVE MODE ENABLED - ACTUAL DELETION WILL OCCUR!")
    
    cleaner = OdfCleaner(dry_run=dry_run)
    success = cleaner.cleanup()
    
    return 0 if success else 1


if __name__ == "__main__":
    exit(main()) 