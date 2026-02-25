#!/usr/bin/env python3
"""Deletes ODF objects based on a LAB GUID using a hierarchical tree approach.

Author:  yvarbev@redhat.com, gh:@yvarbanov
Version: 25.08.04
"""

import rbd
import rados
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
    
    def _clear_dependency_cache(self):
        """Clear cached dependency analysis"""
        self._active_to_trash_dependencies = None
        self._failed_trash_restorations = set()
    
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
        
        return csi_snaps
    
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
                        descendants = list(img.list_descendants())
                        
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
                                # Determine image type based on name
                                desc_image_type = ImageType.CSI_SNAP if 'csi-snap' in desc_name else ImageType.VOLUME
                                new_image = self._create_image_from_rbd(desc_name, desc_image_type)
                                if new_image:
                                    new_image.parent_name = image.name
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
    
    def _create_image_from_rbd(self, img_name: str, image_type: ImageType) -> Optional[OdfImage]:
        """Create an OdfImage from an RBD image"""
        try:
            with rbd.Image(self.ioctx, img_name) as img:
                # Get image info
                stat = img.stat()
                
                # Get creation time if available
                creation_time = None
                try:
                    timestamp = stat.get('timestamp', 0)
                    # Check if timestamp is valid (not epoch 0)
                    if timestamp and timestamp > 0:
                        creation_time = str(datetime.fromtimestamp(timestamp))
                    else:
                        creation_time = "Unknown"
                except Exception as ts_err:
                    creation_time = "Unknown"
                
                # Get parent info
                parent_name = None
                try:
                    parent_info = img.parent_info()
                    if parent_info:
                        parent_name = parent_info[1]
                except:
                    pass
                
                # Get internal snapshots
                internal_snaps = [snap['name'] for snap in img.list_snaps()]
                
                # Check if any snapshots are protected
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
                
        except Exception as e:
            print(f"Error creating image for {img_name}: {e}")
            return None
    
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
        
        try:
            # Handle trash items first - restore them temporarily
            if image.in_trash:
                if not self._restore_from_trash(image):
                    # Failed to restore - skip this trash item but don't fail overall cleanup
                    print(f"  SKIPPED: Could not restore {image.name}, leaving in trash")
                    self._failed_trash_restorations.add(image.name)
                    return True  # Consider this "successful" to continue cleanup
                # After restoration, treat as active image for deletion
            
            # Handle multi-phase operations or fallback flattening
            if image.needs_flattening or self._needs_fallback_flattening(image):
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
    
    def _remove_active_image(self, image: OdfImage) -> bool:
        """Remove an active RBD image (volumes, csi-snaps)"""
        try:
            with rbd.Image(self.ioctx, image.name) as img:
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
    
    def cleanup(self):
        """Main cleanup orchestration"""
        if not self.connect():
            return False
        
        try:
            # Discovery phase
            discovered_items = self.discover_images()
            if not discovered_items:
                print("No items found for cleanup")
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
        return 1
    
    # Check for dry run mode
    dry_run = os.environ.get('DRY_RUN', 'true').lower() in ['true', '1', 'yes']
    
    # Show current configuration
    print(f"Configuration:")
    print(f"  LAB GUID: {os.environ['CL_LAB']}")
    print(f"  Pool: {os.environ['CL_POOL']}")
    print(f"  Dry Run: {'YES' if dry_run else 'NO'}")
    print(f"  Debug: {os.environ.get('DEBUG', 'false').upper()}")
    
    if not dry_run:
        print(f"\nWARNING: LIVE MODE ENABLED - ACTUAL DELETION WILL OCCUR!")
    
    cleaner = OdfCleaner(dry_run=dry_run)
    success = cleaner.cleanup()
    
    return 0 if success else 1


if __name__ == "__main__":
    exit(main()) 