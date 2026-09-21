#!/usr/bin/env python3
"""Discovers and classifies stranded RBD descendant chains hanging off a lab's
volume (the "still has active descendants" case odf-cleanup.py refuses to
delete). DRY_RUN defaults to true (discovery-only); set DRY_RUN=false to
actually remove chains classified SAFE_TO_REMOVE and confirmed phantom
entries - same convention as odf-cleanup.py.

Requirements:
- Python packages: pip install rados rbd
- ODF cluster credentials (CL_CONF, CL_KEYRING environment variables)

Usage:
    export CL_LAB="your-guid"           # bare GUID, or full "{config}-{guid}" prefix, OR
    export CL_VOLUME="some-image"       # a specific image name to inspect directly, OR
    export CL_CLEANUP_LIST="path.txt"   # file of pre-vetted image names (one per line)
                                         # to remove directly, no chain-walking/classify()
    export DRY_RUN="false"              # actually delete what's classified/listed safe (default: true).
                                         # Also auto-repairs orphaned clones invisible to rbd ls/trash
                                         # ls when found - see Documentation/odf-descendant-reaper.md
    export CL_RBD_NAMESPACE="ns-name"   # optional: RBD namespace within the pool (Ceph multi-tenancy,
                                         # distinct from k8s namespaces) some provisioners use instead
                                         # of the pool's default namespace
    python3 utils/odf-descendant-reaper.py

Author:  gh:@yordangit
Version: 26.09.18
"""

import rbd
import rados
import os
import re
import struct
import subprocess
from datetime import datetime
from typing import List, Dict, Optional, Iterator


CLUSTER_NAME_PREFIXES = ('ocp4-cluster', 'openshift-cluster')
VOLUME_GUID_PATTERN = re.compile(r'(?:' + '|'.join(CLUSTER_NAME_PREFIXES) + r')-([a-z0-9]+)-[a-f0-9-]+')

# Safety valve: how deep a descendant chain can go before we give up trying
# to fully resolve it and force it into NEEDS_REVIEW instead of guessing.
MAX_CHAIN_DEPTH = int(os.environ.get('MAX_CHAIN_DEPTH', '10'))


class ChainNode:
    """One image in a descendant chain rooted at an orphaned volume."""

    def __init__(self, name: str, depth: int):
        self.name = name
        self.depth = depth
        self.watchers: List[Dict] = []
        self.children: List['ChainNode'] = []
        self.create_timestamp: Optional[datetime] = None
        self.access_timestamp: Optional[datetime] = None
        self.modify_timestamp: Optional[datetime] = None
        self.snapshot_count: int = 0
        self.snapshots: List[Dict] = []  # [{'name', 'id', 'protected', 'is_trash'}]
        self.error: Optional[str] = None
        self.truncated: bool = False  # hit MAX_CHAIN_DEPTH before fully resolving
        self.phantom_image_id: Optional[str] = None  # set if error is a confirmed phantom entry

    @property
    def has_watcher(self) -> bool:
        return len(self.watchers) > 0

    def all_nodes(self) -> Iterator['ChainNode']:
        """Flatten this node plus every descendant beneath it."""
        yield self
        for child in self.children:
            yield from child.all_nodes()


class DescendantReaper:
    """Connects to ODF and walks/classifies descendant chains for a GUID or image."""

    def __init__(self, debug: bool = False):
        self.debug = debug
        self._phantom_pattern = None  # set by _check_phantom_entry(): 'phantom' or 'missing_rbdid'
        self.ioctx = None
        self.pool_name = None
        self.cluster = None
        self.conf_file = None
        self.keyring = None
        self.client_name = None
        self.my_instance_id = None

    def connect(self) -> bool:
        """Connect to ODF cluster using the same env vars as the other tools."""
        try:
            self.pool_name = os.environ['CL_POOL']
            self.conf_file = os.environ['CL_CONF']
            self.keyring = os.environ['CL_KEYRING']
            with open(self.keyring, 'r') as f:
                for line in f:
                    if line.strip().startswith('[client.') and line.strip().endswith(']'):
                        self.client_name = line.strip()[1:-1]
                        break
                else:
                    raise ValueError(f"No [client.name] found in keyring file: {self.keyring}")
            self.cluster = rados.Rados(conffile=self.conf_file, conf=dict(keyring=self.keyring), name=self.client_name)
            self.cluster.connect()
            self.ioctx = self.cluster.open_ioctx(self.pool_name)
            # Needed so we can filter our own watch out of watcher lists -
            # opening an image ourselves to inspect it registers a watch too.
            self.my_instance_id = self.cluster.get_instance_id()

            # Optional: some provisioners isolate a lab's images into their
            # own RBD namespace
            rbd_namespace = os.environ.get('CL_RBD_NAMESPACE', '')
            if rbd_namespace:
                self.ioctx.set_namespace(rbd_namespace)

            if self.debug:
                print(f"[v] Connected to ODF cluster: {self.cluster.get_fsid()}")
                print(f"  Pool: {self.pool_name}" + (f" (RBD namespace: {rbd_namespace})" if rbd_namespace else ""))

            return True

        except KeyError as e:
            print(f"[x] Error: Missing environment variable {e}")
            print("  Required: CL_POOL, CL_CONF, CL_KEYRING")
            return False
        except Exception as e:
            print(f"[x] Error connecting to ODF cluster: {e}")
            return False

    def find_volumes_for_guid(self, guid: str) -> List[str]:
        """Find active (non-csi-snap) volumes in the pool belonging to a GUID."""
        matches = []
        try:
            for name in rbd.RBD().list(self.ioctx):
                if 'csi-snap' in name:
                    continue
                if name.startswith(guid + '-'):
                    matches.append(name)
                    continue
                match = VOLUME_GUID_PATTERN.search(name)
                if match and match.group(1) == guid:
                    matches.append(name)
        except Exception as e:
            print(f"[x] Error listing images: {e}")
        return matches

    def _length_prefixed(self, s: str) -> bytes:
        """Encode a string the way rbd_directory/rbd_id.<name> values are
        stored: 4-byte LE length + raw bytes. Confirmed byte-for-byte against
        real cluster output - see Documentation/odf-descendant-reaper.md."""
        b = s.encode()
        return struct.pack('<I', len(b)) + b

    def _check_phantom_entry(self, image_name: str) -> Optional[str]:
        """Detects two known corruption patterns where an image can't be
        opened despite still existing in some form (see Documentation/
        odf-descendant-reaper.md): 'phantom' (rbd_id.<name> exists, points to
        an id whose rbd_header is missing
        and 'missing_rbdid' (rbd_id.<name> itself is gone, but rbd_directory
        still resolves a name->id mapping whose rbd_header genuinely exists -
        just the pointer object needs recreating). Sets self._phantom_pattern
        for _format_phantom_message()/_execute_phantom_cleanup() to use."""
        self._phantom_pattern = None
        id_obj = f"rbd_id.{image_name}"

        try:
            self.ioctx.stat(id_obj)
        except Exception:
            return self._check_missing_rbdid(image_name)  # no id pointer - try the other direction

        try:
            raw = self.ioctx.read(id_obj, length=4096)
            length = struct.unpack('<I', raw[:4])[0]
            image_id = raw[4:4 + length].decode('utf-8', errors='replace')
        except Exception as e:
            if self.debug:
                print(f"    DEBUG: found {id_obj} but could not decode its id value: {e}")
            return None

        try:
            self.ioctx.stat(f"rbd_header.{image_id}")
            return None  # header exists - not this pattern, something else is wrong
        except Exception:
            self._phantom_pattern = 'phantom'
            return image_id

    def _check_missing_rbdid(self, image_name: str) -> Optional[str]:
        """The inverse of a phantom entry: rbd_id.<name> is gone, but
        rbd_directory's name_<name> omap key still resolves to an id whose
        rbd_header genuinely exists - a real, valid image just missing its
        convenience name->id pointer. Safer/simpler to repair than an
        orphaned clone since two independent sources already agree on the id."""
        raw = self._get_omap_value("rbd_directory", f"name_{image_name}")
        if raw is None:
            return None  # genuinely gone, not a known pattern
        try:
            length = struct.unpack_from('<I', raw, 0)[0]
            image_id = raw[4:4 + length].decode('utf-8', errors='replace')
            self.ioctx.stat(f"rbd_header.{image_id}")
        except Exception:
            return None
        self._phantom_pattern = 'missing_rbdid'
        return image_id

    def _format_phantom_message(self, image_name: str, image_id: str) -> str:
        """Diagnosis (+ manual rados commands where applicable) for a
        confirmed corruption pattern, used when not auto-executing the fix."""
        if self._phantom_pattern == 'missing_rbdid':
            return (
                f"MISSING rbd_id POINTER - SAFE TO REPAIR (rbd_directory and rbd_header both "
                f"confirm id={image_id} is a real image - only the rbd_id.{image_name} pointer "
                f"rbd.Image() needs to open by name is gone)\n"
                f"    Re-run with DRY_RUN=false to recreate it automatically"
            )
        id_obj = f"rbd_id.{image_name}"
        return (
            f"PHANTOM ENTRY - SAFE TO CLEAN UP (pointer only, no data exists, cannot be repaired)\n"
            f"    {id_obj} -> id={image_id}, but rbd_header.{image_id} is missing\n"
            f"    rados -p {self.pool_name} rmomapkey rbd_directory name_{image_name}\n"
            f"    rados -p {self.pool_name} rmomapkey rbd_directory id_{image_id}\n"
            f"    rados -p {self.pool_name} rm {id_obj}"
        )

    def _execute_phantom_cleanup(self, image_name: str, image_id: str) -> bool:
        """Actually applies whichever pattern _check_phantom_entry() last
        confirmed: recreates the missing rbd_id pointer ('missing_rbdid'), or
        removes the dangling rbd_id + rbd_directory entries ('phantom') -
        never touches rbd_header/data objects either way."""
        if self._phantom_pattern == 'missing_rbdid':
            try:
                self.ioctx.write_full(f"rbd_id.{image_name}", self._length_prefixed(image_id))
                print(f"    [v] Repaired missing rbd_id pointer: {image_name} (id={image_id})")
                return True
            except Exception as e:
                print(f"    [x] Failed to repair rbd_id pointer for {image_name}: {e}")
                return False

        id_obj = f"rbd_id.{image_name}"
        try:
            write_op = self.ioctx.create_write_op()
            self.ioctx.remove_omap_keys(write_op, (f"name_{image_name}", f"id_{image_id}"))
            self.ioctx.operate_write_op(write_op, "rbd_directory")
            write_op.release()
            self.ioctx.remove_object(id_obj)
            print(f"    [v] Removed phantom entry: {image_name} (id={image_id})")
            return True
        except Exception as e:
            print(f"    [x] Failed to clean up phantom entry {image_name}: {e}")
            return False

    def _get_omap_value(self, obj: str, key: str) -> Optional[bytes]:
        """Read one raw omap value. Tries the read_op binding first, falls
        back to the rados CLI (the only path actually validated live against
        this cluster - see Documentation/odf-descendant-reaper.md)."""
        try:
            read_op = self.ioctx.create_read_op()
            it, _ = self.ioctx.get_omap_vals_by_keys(read_op, (key,))
            self.ioctx.operate_read_op(read_op, obj)
            value = dict(it).get(key)
            read_op.release()
            if value is not None:
                return value
        except Exception as e:
            if self.debug:
                print(f"    DEBUG: native omap read failed for {obj}/{key}: {e}")
        try:
            result = subprocess.run(['rados', '-p', self.pool_name, 'getomapval', obj, key, '-'],
                                     capture_output=True, timeout=30)
            return result.stdout if result.returncode == 0 and result.stdout else None
        except Exception as e:
            if self.debug:
                print(f"    DEBUG: rados CLI omap read failed for {obj}/{key}: {e}")
            return None

    def _decode_child_image_specs(self, raw: bytes) -> List[Dict]:
        """Decode a snap_children_<snapid> omap value: a set of
        {pool_id, image_id, pool_namespace} entries RBD's clone-v2 tracking
        keeps on the PARENT's own header, independent of list_children2().
        Byte layout reverse-engineered from real cluster output (not from
        Ceph source) - see Documentation/odf-descendant-reaper.md. Re-verify
        if this ever stops matching."""
        specs = []
        try:
            count = struct.unpack_from('<I', raw, 0)[0]
            offset = 4
            for _ in range(count):
                offset += 2  # struct version header, unused
                body_len = struct.unpack_from('<I', raw, offset)[0]
                body_start = offset + 4
                pool_id = struct.unpack_from('<q', raw, body_start)[0]
                o = body_start + 8
                id_len = struct.unpack_from('<I', raw, o)[0]
                o += 4
                image_id = raw[o:o + id_len].decode('utf-8')
                o += id_len
                ns_len = struct.unpack_from('<I', raw, o)[0]
                o += 4
                namespace = raw[o:o + ns_len].decode('utf-8', errors='replace')
                specs.append({'pool_id': pool_id, 'image_id': image_id, 'namespace': namespace})
                offset = body_start + body_len
        except Exception as e:
            if self.debug:
                print(f"    DEBUG: could not decode ChildImageSpec set: {e}")
            return []
        return specs

    def _diagnose_and_repair_orphan(self, image_name: str, snap_id: int, execute: bool) -> bool:
        """When list_children2() fails on a trashed snapshot, this reads the
        clone-v2 child pointer straight off the parent's header omap -
        bypassing list_children2()/list_descendants() entirely - to find a
        real child image that exists (valid header/data) but is invisible to
        every listing API and to `rbd ls`/`rbd trash ls`. Only actually
        removes anything if execute=True. Full manual procedure this
        automates: Documentation/odf-descendant-reaper.md. Returns True if a
        repair was made (worth retrying the caller's list_children2())."""
        try:
            with rbd.Image(self.ioctx, image_name) as img:
                parent_id = img.id()
        except Exception as e:
            print(f"    [x] Orphan diagnosis: could not reopen {image_name}: {e}")
            return False

        key = f"snap_children_{snap_id:016x}"
        raw = self._get_omap_value(f"rbd_header.{parent_id}", key)
        if raw is None:
            print(f"    [x] Orphan diagnosis: no {key} on {image_name} (id={parent_id}) - not this pattern")
            return False

        specs = self._decode_child_image_specs(raw)
        if not specs:
            print(f"    [x] Orphan diagnosis: found {key} on {image_name} but could not decode it")
            return False

        repaired_any = False
        for spec in specs:
            child_id = spec['image_id']
            print(f"    Orphan diagnosis: {image_name}'s snap {snap_id} still internally references "
                  f"child id={child_id} (pool_id={spec['pool_id']}) - invisible to list_children2()/rbd ls")
            if execute:
                if self._repair_orphaned_clone(child_id):
                    repaired_any = True
            else:
                print("    (set DRY_RUN=false to attempt removal)")
        return repaired_any

    def _repair_orphaned_clone(self, orphan_id: str) -> bool:
        """Confirm orphan_id matches the known pattern (header exists, no
        rbd_directory entry, not sitting in rbd trash, itself childless and
        snapshot-free) and, if so, temporarily relink it into rbd_directory,
        remove it via the normal API (which correctly detaches it from its
        parent), then drop the temporary link. Rolls back on any unexpected
        state rather than guessing. See Documentation/odf-descendant-
        reaper.md for the full manual procedure this automates."""
        name = f"orphan-recovery-{orphan_id}"

        if self._get_omap_value("rbd_directory", f"id_{orphan_id}") is not None:
            print(f"    [x] {orphan_id} already has a rbd_directory entry - not an orphan, skipping")
            return False
        try:
            self.ioctx.stat(f"rbd_header.{orphan_id}")
        except Exception:
            print(f"    [x] {orphan_id} has no rbd_header - different corruption pattern "
                  f"(phantom entry, not orphaned clone), skipping")
            return False
        try:
            if any(t.get('id') == orphan_id for t in rbd.RBD().trash_list(self.ioctx)):
                print(f"    [x] {orphan_id} is sitting in rbd trash - that's normal, not our "
                      f"corruption pattern, skipping")
                return False
        except Exception:
            pass  # can't check trash - proceed, the leaf checks below still protect us

        try:
            write_op = self.ioctx.create_write_op()
            self.ioctx.set_omap(write_op, (f"name_{name}",), (self._length_prefixed(orphan_id),))
            self.ioctx.operate_write_op(write_op, "rbd_directory")
            write_op.release()
            write_op = self.ioctx.create_write_op()
            self.ioctx.set_omap(write_op, (f"id_{orphan_id}",), (self._length_prefixed(name),))
            self.ioctx.operate_write_op(write_op, "rbd_directory")
            write_op.release()
            self.ioctx.write_full(f"rbd_id.{name}", self._length_prefixed(orphan_id))
        except Exception as e:
            print(f"    [x] Could not relink orphan {orphan_id}: {e}")
            self._rollback_orphan_relink(orphan_id, name)
            return False

        # Re-verify through the NORMAL API now that it's nameable - never
        # trust the raw omap read alone for the final safety check.
        try:
            with rbd.Image(self.ioctx, name) as img:
                if list(img.list_snaps()):
                    print(f"    [x] Orphan {orphan_id} has its own snapshot(s) - too deep to "
                          f"auto-repair, needs manual review")
                    self._rollback_orphan_relink(orphan_id, name)
                    return False
                try:
                    if list(img.list_descendants()):
                        print(f"    [x] Orphan {orphan_id} has its own children - too deep to auto-repair")
                        self._rollback_orphan_relink(orphan_id, name)
                        return False
                except Exception:
                    print(f"    [x] Orphan {orphan_id}: could not verify it's childless - not auto-repairing")
                    self._rollback_orphan_relink(orphan_id, name)
                    return False
                if self._get_watchers(img, name):
                    print(f"    [x] Orphan {orphan_id} has active watcher(s) - not auto-repairing")
                    self._rollback_orphan_relink(orphan_id, name)
                    return False
        except Exception as e:
            print(f"    [x] Could not re-verify relinked orphan {orphan_id}: {e}")
            self._rollback_orphan_relink(orphan_id, name)
            return False

        try:
            rbd.RBD().remove(self.ioctx, name)
            print(f"    [v] Removed orphaned clone {orphan_id} (was invisible to rbd ls/trash ls)")
            return True
        except Exception as e:
            print(f"    [x] Failed to remove relinked orphan {orphan_id}: {e}")
            self._rollback_orphan_relink(orphan_id, name)
            return False

    def _rollback_orphan_relink(self, orphan_id: str, name: str):
        """Undo the temporary rbd_directory relink without touching the real
        header/data object - used whenever auto-repair aborts partway through."""
        try:
            write_op = self.ioctx.create_write_op()
            self.ioctx.remove_omap_keys(write_op, (f"name_{name}", f"id_{orphan_id}"))
            self.ioctx.operate_write_op(write_op, "rbd_directory")
            write_op.release()
            self.ioctx.remove_object(f"rbd_id.{name}")
        except Exception:
            pass

    # Name varies by packaging: docs say list_watchers(), RHEL9's
    # python3-rbd 18.2.8 exposes watchers_list() instead. Try both.
    WATCHER_METHOD_NAMES = ('list_watchers', 'watchers_list')

    def _get_timestamp(self, img, method_name: str) -> Optional[datetime]:
        """Handles either a datetime or raw epoch return, depending on Ceph client binding version."""
        method = getattr(img, method_name, None)
        if method is None:
            return None
        try:
            value = method()
        except Exception as e:
            if self.debug:
                print(f"    DEBUG: {method_name}() failed: {e}")
            return None
        if isinstance(value, datetime):
            return value
        if isinstance(value, (int, float)) and value > 0:
            return datetime.fromtimestamp(value)
        return None

    # Same story as watchers: try both parent_info() (tuple) and parent() (dict).
    def _get_parent_name(self, img) -> Optional[str]:
        """Get the immediate parent image's name for an open rbd.Image, if any."""
        parent_info = getattr(img, 'parent_info', None)
        if parent_info is not None:
            try:
                _, image_name, _ = parent_info()
                return image_name
            except Exception:
                pass
        parent = getattr(img, 'parent', None)
        if parent is not None:
            try:
                info = parent()
                return info.get('image_name') if isinstance(info, dict) else None
            except Exception:
                pass
        return None

    def _is_protected_snap(self, img, snap_name: str) -> Optional[bool]:
        method = getattr(img, 'is_protected_snap', None)
        if method is None:
            return None
        try:
            return bool(method(snap_name))
        except Exception:
            return None

    def _format_watcher(self, w) -> str:
        if isinstance(w, dict):
            return f"watcher={w.get('addr', '?')} client.{w.get('id', '?')} cookie={w.get('cookie', '?')}"
        return str(w)

    def _get_watchers(self, img, image_name: str) -> List[str]:
        """List watchers, excluding our own connection's watch (opening the
        image to inspect it registers one too)."""
        for method_name in self.WATCHER_METHOD_NAMES:
            method = getattr(img, method_name, None)
            if method is None:
                continue
            raw_watchers = list(method())
            external = [
                w for w in raw_watchers
                if not (isinstance(w, dict) and w.get('id') == self.my_instance_id)
            ]
            if self.debug and len(external) != len(raw_watchers):
                print(f"    DEBUG: filtered out our own watch on {image_name} "
                      f"({len(raw_watchers)} raw -> {len(external)} external)")
            return [self._format_watcher(w) for w in external]

        raise RuntimeError(f"none of {self.WATCHER_METHOD_NAMES} found on rbd.Image "
                            f"for {image_name} - unknown Ceph client binding")

    def _inspect_node(self, image_name: str) -> (ChainNode, Optional[str]):
        """Open one image, collect its watcher/timestamp/snapshot info, and
        return its parent's name (no recursion - see analyze_volume)."""
        node = ChainNode(image_name, depth=0)  # depth is fixed up by the caller
        parent_name = None

        try:
            with rbd.Image(self.ioctx, image_name) as img:
                # NOTE: on Ceph 18.2.8 (reef)/python3-rbd, create_timestamp()
                # has consistently come back exactly 5h ahead of access_
                # timestamp()/modify_timestamp().
                # Unconfirmed whether this is specific to this Ceph/client
                # version, no correction applied.
                node.create_timestamp = self._get_timestamp(img, 'create_timestamp')
                node.access_timestamp = self._get_timestamp(img, 'access_timestamp')
                node.modify_timestamp = self._get_timestamp(img, 'modify_timestamp')

                try:
                    node.snapshots = [
                        {
                            'name': s.get('name'), 'id': s.get('id'),
                            'protected': self._is_protected_snap(img, s.get('name')),
                            # RBD "clone v2" moves a deleted snapshot with live clones into
                            # a trash namespace instead of blocking the delete - it's then
                            # only reachable by id, not by name (confirmed against real
                            # cluster output: even `rbd snap rm --force` can't find it by name).
                            'is_trash': 'trash' in s,
                        }
                        for s in img.list_snaps() if s.get('name')
                    ]
                    node.snapshot_count = len(node.snapshots)
                except Exception:
                    pass

                try:
                    node.watchers = self._get_watchers(img, image_name)
                except Exception as e:
                    # Treat "can't tell" as NEEDS_REVIEW territory, not as "no watchers"
                    node.error = f"could not check watchers: {e}"

                parent_name = self._get_parent_name(img)

        except Exception as e:
            node.error = f"could not open image: {e}"
            phantom_id = self._check_phantom_entry(image_name)
            if phantom_id:
                node.phantom_image_id = phantom_id
                node.error += f"\n    {self._format_phantom_message(image_name, phantom_id)}"

        return node, parent_name

    def classify(self, root: ChainNode) -> str:
        """SAFE_TO_REMOVE only if every node in the chain has zero watchers,
        no errors, and wasn't truncated by the depth cap."""
        for node in root.all_nodes():
            if node.error or node.truncated or node.has_watcher:
                return 'NEEDS_REVIEW'
        return 'SAFE_TO_REMOVE'

    def classify_reason(self, root: ChainNode) -> str:
        """Human-readable reason for a NEEDS_REVIEW classification."""
        reasons = []
        for node in root.all_nodes():
            if node.has_watcher:
                reasons.append(f"{node.name} has an active watcher (in use right now)")
            if node.error:
                reasons.append(f"{node.name}: {node.error}")
            if node.truncated:
                reasons.append(f"{node.name}: chain exceeds MAX_CHAIN_DEPTH={MAX_CHAIN_DEPTH}, stopped resolving")
        return "; ".join(reasons) if reasons else "unknown"

    def _direct_children_trash_safe(self, img, image_name: str, execute: bool = False) -> (List[Dict], bool):
        """One level of children via set_snap()/set_snap_by_id() per snapshot +
        list_children2() - works even when a snapshot is in RBD's trash
        namespace, which makes list_descendants() fail outright (confirmed
        against real cluster output: a trashed snap can still have a live
        child, which is exactly why it's still sitting there). Returns
        (children, ok) - ok=False means at least one snapshot's children
        couldn't be listed (confirmed this can happen even via set_snap_by_id)
        - that's "unknown", never treat it as "confirmed no children"."""
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
                # Can be a genuinely unresolvable case, or the "orphaned
                # clone" corruption pattern documented in
                # Documentation/odf-descendant-reaper.md - try to auto-repair
                # it before giving up (actual removal still needs execute=True).
                repaired = 'trash' in snap and self._diagnose_and_repair_orphan(image_name, snap['id'], execute)
                if repaired:
                    try:
                        children.extend(img.list_children2())
                    except Exception:
                        ok = False
                else:
                    ok = False
            finally:
                try:
                    img.set_snap(None)
                except Exception:
                    pass
        return children, ok

    def _walk_descendants_trash_safe(self, root_name: str, execute: bool = False) -> (List[Dict], bool):
        """Recursive fallback for list_descendants() - only used when it fails
        outright. list_children2() is single-level, so this does its own BFS.
        Returns (children, ok) - ok=False if any node's children couldn't be
        fully resolved (must not be reported as a clean/empty result)."""
        all_children = []
        seen = set()
        all_ok = True
        queue = [root_name]
        while queue:
            name = queue.pop(0)
            try:
                with rbd.Image(self.ioctx, name) as img:
                    direct, ok = self._direct_children_trash_safe(img, name, execute)
            except Exception:
                all_ok = False
                continue
            if not ok:
                all_ok = False
            for c in direct:
                child_name = c.get('image') or c.get('name')
                if not child_name or child_name in seen:
                    continue
                seen.add(child_name)
                if not c.get('trash', False):
                    all_children.append(c)
                    queue.append(child_name)
        return all_children, all_ok

    def analyze_volume(self, volume_name: str, execute: bool = False) -> (List[ChainNode], Optional[str], Optional[str]):
        """Returns (roots, error, phantom_image_id). error != None means the
        volume couldn't be opened - not the same as "zero descendants"."""
        try:
            with rbd.Image(self.ioctx, volume_name) as img:
                try:
                    flat = [d for d in img.list_descendants() if not d.get('trash', False)]
                except Exception:
                    flat, ok = self._walk_descendants_trash_safe(volume_name, execute)
                    if not ok:
                        raise RuntimeError("could not fully resolve descendants (trash-safe walk incomplete)")
        except Exception as e:
            error = f"could not read descendants of {volume_name}: {e}"
            phantom_id = self._check_phantom_entry(volume_name)
            if phantom_id:
                error += f"\n    {self._format_phantom_message(volume_name, phantom_id)}"
            return [], error, phantom_id

        names = []
        for desc in flat:
            name = desc.get('name') or desc.get('image')
            if name:
                names.append(name)

        if not names:
            return [], None, None

        nodes: Dict[str, ChainNode] = {}
        parent_of: Dict[str, str] = {}
        for name in names:
            node, parent_name = self._inspect_node(name)
            nodes[name] = node
            if parent_name:
                parent_of[name] = parent_name

        # A node is a top-level chain if its parent isn't in our descendant
        # set (i.e. the parent is the volume itself).
        roots = []
        for name, node in nodes.items():
            parent_name = parent_of.get(name)
            if parent_name and parent_name in nodes:
                nodes[parent_name].children.append(node)
            else:
                roots.append(node)

        def _finalize_depth(node: ChainNode, depth: int):
            node.depth = depth
            if depth >= MAX_CHAIN_DEPTH:
                node.truncated = True  # flag pathologically deep chains for review
            for child in node.children:
                _finalize_depth(child, depth + 1)

        for root in roots:
            _finalize_depth(root, depth=1)

        return roots, None, None

    def print_chain(self, node: ChainNode, prefix: str = "  "):
        """Print one node and its children as an indented tree."""
        watcher_str = f"WATCHERS: {len(node.watchers)}" if node.has_watcher else "watchers: none"
        created_str = f"created: {node.create_timestamp}" if node.create_timestamp else "created: unknown"
        accessed_str = f"accessed: {node.access_timestamp}" if node.access_timestamp else "accessed: unknown"
        modified_str = f"modified: {node.modify_timestamp}" if node.modify_timestamp else "modified: unknown"
        ts_str = f"{created_str}, {accessed_str}, {modified_str}"
        flags = []
        if node.error:
            flags.append(f"ERROR: {node.error}")
        if node.truncated:
            flags.append("TRUNCATED (depth cap reached)")
        flag_str = f" [{', '.join(flags)}]" if flags else ""

        protected_count = sum(1 for s in node.snapshots if s['protected'])
        snaps_str = f"snaps: {node.snapshot_count}" + (f" ({protected_count} protected)" if protected_count else "")

        print(f"{prefix}{node.name} ({watcher_str}, {ts_str}, {snaps_str}){flag_str}")
        for child in node.children:
            self.print_chain(child, prefix + "  ")

    def removal_order_lines(self, root: ChainNode) -> List[str]:
        """Leaf-first removal commands: unprotect+rm any snapshots on a
        node before rbd rm'ing the node itself."""
        lines = []
        order = list(root.all_nodes())[::-1]  # children/leaves first, root last
        for node in order:
            for snap in node.snapshots:
                snap_ref = f"{self.pool_name}/{node.name}@{snap['name']}"
                if snap['is_trash']:
                    # Not reachable by name at all (even --force) - only removable
                    # by id via the Python binding's remove_snap_by_id().
                    lines.append(f"# trashed snap (id={snap['id']}) on {snap_ref} - "
                                 f"remove via: rbd.Image(ioctx, '{node.name}').remove_snap_by_id({snap['id']})")
                    continue
                if snap['protected'] is not False:  # True, or unknown - try it
                    lines.append(f"rbd snap unprotect {snap_ref}   # skip if this errors as already unprotected")
                lines.append(f"rbd snap rm {snap_ref}")
            lines.append(f"rbd rm {self.pool_name}/{node.name}")
        return lines

    def print_removal_order(self, root: ChainNode):
        for line in self.removal_order_lines(root):
            print(f"    {line}")

    def _execute_chain_removal(self, root: ChainNode) -> bool:
        """Actually perform the leaf-first removal for a SAFE_TO_REMOVE chain
        via the rbd/rados bindings. Returns True only if every node in the
        chain was fully removed; stops at the first failure."""
        order = list(root.all_nodes())[::-1]  # leaves first, root last
        for node in order:
            try:
                with rbd.Image(self.ioctx, node.name) as img:
                    for snap in node.snapshots:
                        if snap['is_trash']:
                            # Only reachable by id - name-based lookup always
                            # resolves to the user namespace, never trash.
                            img.remove_snap_by_id(snap['id'])
                            continue
                        if snap['protected'] is not False:  # True, or unknown - try it
                            try:
                                img.unprotect_snap(snap['name'])
                            except Exception:
                                pass  # already unprotected - fine
                        img.remove_snap(snap['name'])
            except Exception as e:
                print(f"    [x] Failed to process snapshots for {node.name}: {e}")
                return False
            try:
                rbd.RBD().remove(self.ioctx, node.name)
                print(f"    [v] Removed {node.name}")
            except Exception as e:
                print(f"    [x] Failed to remove {node.name}: {e}")
                return False
        return True

    def analyze_guid(self, guid: Optional[str], volume_name: Optional[str], execute: bool = False) -> str:
        """Main analysis entry point: either a specific volume, or all volumes
        for a GUID. Returns a status string: NO_VOLUMES_FOUND / ERROR / CLEAN /
        ALL_SAFE / NEEDS_REVIEW / RESOLVED. ERROR means we couldn't even read a
        volume's descendants - never conflate that with "genuinely has none".
        RESOLVED (execute=True only) means everything blocking was actually
        removed and it's now safe to retry the volume's normal cleanup.

        execute=True actually deletes: chains classified SAFE_TO_REMOVE, and
        confirmed phantom entries (dangling pointer, no data, cause is known).
        Everything else (active watchers, undiagnosed errors, truncated
        chains) is never touched - cause not determined, needs a human."""
        if volume_name:
            volumes = [volume_name]
        else:
            volumes = self.find_volumes_for_guid(guid)
            if not volumes:
                print(f"[x] No active volumes found in pool for GUID: {guid}")
                return 'NO_VOLUMES_FOUND'

        print("=" * 80)
        print("DESCENDANT CHAIN ANALYSIS" + (" - LIVE: EXECUTING SAFE REMOVALS" if execute else ""))
        print("=" * 80)

        any_chains = False
        any_error = False
        every_volume_all_safe = True

        for vol in volumes:
            print(f"\nVolume: {vol}")
            chains, error, phantom_id = self.analyze_volume(vol, execute=execute)

            if error:
                any_error = True
                print(f"  [x] ERROR: {error}")
                if execute and phantom_id:
                    print(f"  Executing phantom entry cleanup for {vol}...")
                    if self._execute_phantom_cleanup(vol, phantom_id):
                        any_error = False  # resolved - was the only issue for this volume
                    else:
                        print(f"  [x] Phantom cleanup failed - {vol} still needs review")
                continue

            if not chains:
                print("  [v] No active descendants - clean orphan, nothing to review")
                continue

            any_chains = True
            all_safe = True
            for chain_root in chains:
                classification = self.classify(chain_root)
                print(f"\n  Descendant chain (root: {chain_root.name}):")
                self.print_chain(chain_root)

                if classification == 'SAFE_TO_REMOVE':
                    print(f"  [v] Classification: SAFE_TO_REMOVE (zero watchers throughout, terminal chain)")
                    if execute:
                        print("  Executing removal...")
                        if not self._execute_chain_removal(chain_root):
                            all_safe = False
                            print(f"  [x] Removal failed partway through - {vol} needs manual review")
                else:
                    all_safe = False
                    reason = self.classify_reason(chain_root)
                    print(f"  [x] Classification: NEEDS_REVIEW - {reason}")

            # Only offer/attempt the volume's own removal once all its
            # descendant chains are safe (odf-cleanup.py would still block it
            # otherwise).
            if all_safe:
                if execute:
                    try:
                        rbd.RBD().remove(self.ioctx, vol)
                        print(f"  [v] Removed volume itself: {vol}")
                    except Exception as e:
                        all_safe = False
                        print(f"  [x] Failed to remove volume {vol}: {e}")
                else:
                    print(f"\n  All descendant chains are SAFE_TO_REMOVE.")
                    print("  Manual removal order (descendants first, volume last):")
                    for chain_root in chains:
                        self.print_removal_order(chain_root)
                    print(f"    rbd rm {self.pool_name}/{vol}   # the volume itself, now that its descendants are gone")

            if not all_safe:
                every_volume_all_safe = False
                print(f"\n  [x] Volume {vol} is NOT safe to remove yet - "
                      f"resolve the NEEDS_REVIEW chain(s) above first")

        print("\n" + "=" * 80)
        if not any_chains and not any_error:
            print("SUMMARY: no descendant chains found - safe to proceed with normal cleanup")
        print("=" * 80)

        if any_error:
            return 'ERROR'
        if not any_chains:
            return 'CLEAN'
        if every_volume_all_safe:
            return 'RESOLVED' if execute else 'ALL_SAFE'
        return 'NEEDS_REVIEW'


    def cleanup_named_images(self, names: List[str], execute: bool) -> bool:
        """Remove specific images already vetted as safe by an external caller
        (e.g. odf-oc-compare.py's k8s-verified SAFE TO DELETE list for
        parentless csi-snap/csi-vol images) - not routed through classify()/
        chain-walking, which has no k8s awareness of its own. Still does one
        fresh watcher + children check per image here, since cluster state
        can change between analysis and execution. Returns False if any
        removal actually failed (skips are not failures)."""
        removed = skipped = failed = 0
        for name in names:
            print(f"\n{name}:")
            try:
                with rbd.Image(self.ioctx, name) as img:
                    watchers = self._get_watchers(img, name)
                    if watchers:
                        print(f"  [x] SKIPPED: now has active watcher(s) {watchers} - state changed since analysis")
                        skipped += 1
                        continue
                    children = [d for d in img.list_descendants() if not d.get('trash', False)]
                    if children:
                        print(f"  [x] SKIPPED: now has {len(children)} descendant(s) - state changed since analysis")
                        skipped += 1
                        continue
                    snaps = [
                        {'name': s.get('name'), 'id': s.get('id'),
                         'protected': self._is_protected_snap(img, s.get('name')),
                         'is_trash': 'trash' in s}
                        for s in img.list_snaps() if s.get('name')
                    ]
            except Exception as e:
                phantom_id = self._check_phantom_entry(name)
                if phantom_id:
                    if execute:
                        if self._execute_phantom_cleanup(name, phantom_id):
                            removed += 1
                        else:
                            failed += 1
                    else:
                        print(f"  {self._format_phantom_message(name, phantom_id)}")
                        removed += 1
                    continue
                print(f"  [x] SKIPPED: could not open image: {e}")
                skipped += 1
                continue

            if not execute:
                for snap in snaps:
                    snap_ref = f"{self.pool_name}/{name}@{snap['name']}"
                    if snap['is_trash']:
                        print(f"    # trashed snap (id={snap['id']}) - remove via remove_snap_by_id(), not rbd CLI")
                        continue
                    if snap['protected'] is not False:
                        print(f"    rbd snap unprotect {snap_ref}   # skip if this errors as already unprotected")
                    print(f"    rbd snap rm {snap_ref}")
                print(f"    rbd rm {self.pool_name}/{name}")
                removed += 1
                continue

            try:
                with rbd.Image(self.ioctx, name) as img:
                    for snap in snaps:
                        if snap['is_trash']:
                            img.remove_snap_by_id(snap['id'])
                            continue
                        if snap['protected'] is not False:
                            try:
                                img.unprotect_snap(snap['name'])
                            except Exception:
                                pass
                        img.remove_snap(snap['name'])
                rbd.RBD().remove(self.ioctx, name)
                print(f"  [v] Removed")
                removed += 1
            except Exception as e:
                print(f"  [x] FAILED to remove: {e}")
                failed += 1

        print("\n" + "=" * 80)
        verb = "removed" if execute else "would be removed"
        print(f"SUMMARY: {removed} {verb}, {skipped} skipped (state changed/unsafe), {failed} failed")
        print("=" * 80)
        return failed == 0


def main():
    """Main entry point"""
    dry_run = os.environ.get('DRY_RUN', 'true').lower() in ['true', '1', 'yes']
    execute = not dry_run
    print(f"ODF Descendant Reaper ({'discovery-only' if dry_run else 'LIVE MODE'})")
    print("=" * 80)

    required_envs = ['CL_POOL', 'CL_CONF', 'CL_KEYRING']
    missing_envs = [env for env in required_envs if env not in os.environ]

    if missing_envs:
        print(f"[x] Error: Missing environment variables: {', '.join(missing_envs)}")
        print("\nRequired environment variables:")
        for env in required_envs:
            print(f"  {env}")
        print("\nAlso required, one of:")
        print("  CL_LAB          - GUID whose volumes should be inspected")
        print("  CL_VOLUME       - a specific image name to inspect directly")
        print("  CL_CLEANUP_LIST - file of pre-vetted image names (one per line) to remove directly")
        print("\nOptional:")
        print("  MAX_CHAIN_DEPTH=N  - depth cap before forcing NEEDS_REVIEW (default: 10)")
        print("  DRY_RUN=[true/false]  - false actually deletes SAFE_TO_REMOVE chains + phantom entries,")
        print("    and auto-repairs orphaned clones invisible to rbd ls/trash ls (default: true)")
        print("  CL_RBD_NAMESPACE   - RBD namespace within the pool (Ceph multi-tenancy, distinct")
        print("    from k8s namespaces) some provisioners isolate a lab's images into (default: pool's default namespace)")
        print("  DEBUG=[true/false]")
        return 1

    guid = os.environ.get('CL_LAB')
    volume_name = os.environ.get('CL_VOLUME')
    cleanup_list_file = os.environ.get('CL_CLEANUP_LIST')

    if not guid and not volume_name and not cleanup_list_file:
        print("[x] Error: Set one of CL_LAB (GUID), CL_VOLUME (image name), or CL_CLEANUP_LIST (file of image names)")
        return 1

    debug = os.environ.get('DEBUG', 'false').lower() in ['true', '1', 'yes']

    if cleanup_list_file:
        target_desc = f"image list from {cleanup_list_file}"
    else:
        target_desc = 'GUID ' + guid if guid else 'Volume ' + volume_name

    print("Configuration:")
    print(f"  Pool: {os.environ['CL_POOL']}")
    if os.environ.get('CL_RBD_NAMESPACE'):
        print(f"  RBD Namespace: {os.environ['CL_RBD_NAMESPACE']}")
    print(f"  Target: {target_desc}")
    print(f"  Max chain depth: {MAX_CHAIN_DEPTH}")
    print(f"  Dry Run: {'YES' if dry_run else 'NO'}")
    print(f"  Debug: {'YES' if debug else 'NO'}")
    print("")

    if not dry_run:
        print("WARNING: LIVE MODE ENABLED - chains classified SAFE_TO_REMOVE and confirmed")
        print("phantom entries will actually be deleted, and orphaned clones invisible to")
        print("rbd ls/trash ls will be auto-repaired when found. NEEDS_REVIEW / undiagnosed")
        print("errors are still never touched. See Documentation/odf-descendant-reaper.md.")
        print("")

    reaper = DescendantReaper(debug=debug)
    if not reaper.connect():
        return 1

    try:
        if cleanup_list_file:
            try:
                with open(cleanup_list_file) as f:
                    names = [line.strip() for line in f if line.strip() and not line.strip().startswith('#')]
            except Exception as e:
                print(f"[x] Error reading {cleanup_list_file}: {e}")
                return 1
            if not names:
                print("[v] Cleanup list is empty - nothing to do")
                return 0
            return 0 if reaper.cleanup_named_images(names, execute=execute) else 1

        status = reaper.analyze_guid(guid, volume_name, execute=execute)
        # Exit 0 only when there's nothing left blocking normal cleanup;
        # non-zero tells a caller (e.g. a wrapper script) this GUID still
        # needs a human, so it doesn't get silently treated as done.
        resolved_statuses = ('NO_VOLUMES_FOUND', 'CLEAN', 'RESOLVED')
        return 0 if status in resolved_statuses else 1
    except Exception as e:
        print(f"[x] Error during analysis: {e}")
        return 1
    finally:
        if reaper.ioctx:
            reaper.ioctx.close()
        if reaper.cluster:
            reaper.cluster.shutdown()


if __name__ == "__main__":
    exit(main())
