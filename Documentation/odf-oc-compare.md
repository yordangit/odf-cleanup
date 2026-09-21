# ODF-OpenShift Comparator Script Documentation

## Overview
The `odf-oc-compare.py` script compares OpenShift namespaces with ODF (OpenShift Data Foundation) RBD images to identify orphaned lab GUIDs. It discovers active lab environments from OpenShift and compares them with ODF storage volumes to find orphaned storage that can be safely cleaned up.

## Execution Flow
```
main() → OdfOpenShiftComparator.run_comparison() → connect_odf() → discover_namespace_guids() → discover_volume_snapshot_contents() → discover_persistent_volumes() → discover_odf_guids() → compare_and_find_orphans() → check_empty_labs() → generate_report() → generate_cleanup_script()
```

### High-Level Execution Flow Diagram

```mermaid
graph TD
    A["Connect to ODF Cluster"] --> B["Discover Namespace GUIDs"]
    B --> B2["Discover VolumeSnapshotContents (non-fatal)"]
    B2 --> B3["Discover PersistentVolumes (non-fatal)"]
    B3 --> C["Discover ODF GUIDs"]
    C --> D["Compare & Find Orphans"]
    D --> D2["Check Empty Labs (PVCs)"]
    D2 --> E["Generate Report"]
    E --> F["Generate Cleanup Script"]
    F --> G["Complete Analysis"]
```

---

## Classes

### 1. `OdfOpenShiftComparator`
**Purpose:** Main orchestrator for comparing ODF volumes with OpenShift namespaces

#### Constructor `__init__()`
- Initializes connection variables and result sets
- Sets up caching for expensive operations (CSI snap lookups, image lists)
- Initializes statistics tracking

#### Key Properties:
- `active_namespace_guids` - Set of GUIDs found in active namespaces
- `odf_guids` - Set of GUIDs found in ODF storage
- `orphaned_guids` - Set of GUIDs present in ODF but not in active namespaces
- `csi_snap_guid_cache` / `csi_vol_guid_cache` - Cache for csi-snap/csi-vol parent lookups
- `parentless_csi_snaps` / `parentless_csi_vols` - Analysis of csi-snaps/csi-vols without parents
- `volume_snapshot_contents` - Cluster's `VolumeSnapshotContent` objects, used to verify real ownership of parentless csi-snaps; `None` means "couldn't load" (callers fail safe into REVIEW), `[]` means "loaded, none exist"
- `persistent_volumes` - Cluster's `PersistentVolume` objects, same idea but for parentless csi-vols (matched via `spec.csi.volumeHandle`)
- `guid_to_namespaces` - Maps each active namespace GUID to the namespace name(s) it was found in
- `empty_labs` - Active namespace GUIDs with zero ODF footprint, checked against real OCP PVCs (see `check_empty_labs()`)

---

## OdfOpenShiftComparator Methods (Execution Order)

### Phase 1: Connection

#### `connect_odf()`
**When:** First step in comparison process
**Purpose:** Establishes connection to ODF cluster using environment variables
**Does:**
- Reads CL_POOL, CL_CONF, CL_KEYRING from environment
- Creates Rados cluster connection and IO context
- Validates client credentials from keyring file
- Returns success/failure status

### Phase 2: Discovery

#### `discover_namespace_guids()`
**When:** After successful ODF connection
**Purpose:** Discovers active lab GUIDs from OpenShift namespaces
**Does:**
- Loads kubeconfig and creates Kubernetes client
- Lists all namespaces in the cluster
- Extracts GUIDs using pattern: `sandbox-{GUID}-*`
- Populates `active_namespace_guids` set
- Updates statistics for namespace discovery

#### `discover_volume_snapshot_contents()`
**When:** After namespace discovery, before ODF discovery
**Purpose:** Loads all `VolumeSnapshotContent` objects so parentless csi-snap ownership can be verified against real Kubernetes state instead of just RBD children
**Does:**
- Lists cluster-scoped `volumesnapshotcontents` via the Kubernetes `CustomObjectsApi`
- Non-fatal on failure: sets `volume_snapshot_contents = None` so callers know to fail safe into `REVIEW` rather than silently treating it as "none exist"

#### `discover_persistent_volumes()`
**When:** After VolumeSnapshotContent discovery, before ODF discovery
**Purpose:** Same idea as above, but for parentless csi-vols - loads all `PersistentVolume` objects so ownership can be checked via `spec.csi.volumeHandle`
**Does:**
- Lists cluster-scoped PVs via the Kubernetes `CoreV1Api`
- Non-fatal on failure: sets `persistent_volumes = None` so callers fail safe into `REVIEW`

#### `discover_odf_guids()`
**When:** After PersistentVolume discovery
**Purpose:** Discovers all lab GUIDs from ODF RBD images and snapshots
**Does:**
- Lists all active RBD images in the pool
- Lists all trash items in the pool
- Processes each image through `_extract_guid_from_image()`
- Handles special case of CSI snapshots via parent lookups
- Caches results for performance optimization
- Updates statistics for ODF discovery

#### Helper Methods for Discovery:
- `_extract_guid_from_image()` - Extracts GUID from image name using regex patterns; for `csi-snap-*`/`csi-vol-*` with no cluster-prefix, delegates to the parent-lookup helpers below (a descendant with a resolvable parent GUID is never treated as parentless)
- `_get_guid_from_csi_snap_parent()` / `_get_guid_from_csi_vol_parent()` - Gets GUID from the image's immediate RBD parent (with caching); triggers the matching `_analyze_parentless_*` call if there's no parent at all
- `_analyze_parentless_csi_snap()` / `_analyze_parentless_csi_vol()` - Analyzes csi-snaps/csi-vols that have no parent
- `_check_csi_snap_k8s_ownership()` - For a parentless csi-snap with zero RBD children, matches its UUID against loaded `VolumeSnapshotContent` `snapshotHandle`s to check whether it's still referenced (and whether that reference's namespace is active)
- `_check_csi_vol_k8s_ownership()` - Same idea for a parentless csi-vol, matching its UUID against `PersistentVolume` `spec.csi.volumeHandle`s
- `_extract_guid_from_name()` - Generic GUID extraction utility

### Phase 3: Analysis

#### `compare_and_find_orphans()`
**When:** After both discovery phases complete
**Purpose:** Compares namespace GUIDs with ODF GUIDs to identify orphans
**Does:**
- Performs set subtraction: `odf_guids - active_namespace_guids`
- Populates `orphaned_guids` set
- Updates orphan statistics
- Provides summary of comparison results

#### Analysis Helper Methods:
- `_count_odf_items_for_guid()` - Counts volumes, snapshots, and trash items per GUID
- `_order_guids_by_complexity()` - Orders orphaned GUIDs by cleanup complexity

#### `check_empty_labs()`
**When:** After `compare_and_find_orphans()`
**Purpose:** For active namespace GUIDs with zero ODF footprint, confirms they're genuinely empty rather than a scan gap
**Does:**
- Computes `active_namespace_guids - odf_guids`
- For each such GUID's namespace(s), lists PVCs via the Kubernetes `CoreV1Api`
- Zero PVCs across all its namespaces → `CONFIRMED EMPTY`; any PVCs found → `HAS PVCS - REVIEW` (unexpected - investigate); listing failure → `ERROR - could not check PVCs`
- Populates `empty_labs`

### Phase 4: Reporting

#### `generate_report()`
**When:** After orphan analysis
**Purpose:** Generates comprehensive comparison report
**Does:**
- Reports orphaned GUIDs ordered by cleanup complexity
- Analyzes parentless CSI snapshots and volumes with recommendations
- Provides detailed statistics summary
- Categorizes findings for actionable insights

#### `generate_cleanup_script()`
**When:** After report generation
**Purpose:** Creates automated bash script for orphan cleanup
**Does:**
- Generates executable shell script with environment setup
- Orders GUIDs by cleanup priority (simple → complex)
- Defaults `DRY_RUN="false"` (live) but prompts for `y/n` confirmation before running anything
- Auto-detects `odf-cleanup.py` and `odf-descendant-reaper.py` at either `./` or `../`/`utils/` so the generated script works whether it's run from the repo root or from `utils/` - no need to copy/move files
- Per GUID, runs `odf-cleanup.py` (which now self-handles watcher checks and phantom entries); on failure it's logged to `needs_descendant_review.txt` for manual investigation with `odf-descendant-reaper.py` instead of being retried automatically
- Appends a section that writes every parentless csi-snap/csi-vol name marked `SAFE TO DELETE` to a file and runs `odf-descendant-reaper.py` in its `CL_CLEANUP_LIST` mode against it (these have no GUID, so `odf-cleanup.py` can't touch them); omitted entirely if there are none
- Script is generated even with zero orphaned GUIDs, as long as there's at least one safe CSI leftover to clean up

---

## Main Entry Point

### `main()`
**Purpose:** Script entry point and configuration validation
**Does:**
- Validates required environment variables (CL_POOL, CL_CONF, CL_KEYRING)
- Configures debug mode
- Creates OdfOpenShiftComparator instance and runs comparison
- Returns exit code based on success/failure

### `run_comparison()`
**Purpose:** Main comparison workflow orchestration
**Does:**
- Coordinates all phases of the comparison process
- Handles exceptions and cleanup
- Ensures proper resource cleanup (connections, IO contexts)

---

## Key Features

### Discovery Capabilities
- **Namespace Pattern Matching:** Extracts GUIDs from `sandbox-{GUID}-*` namespace patterns
- **ODF Volume Recognition:** Identifies volumes using `{ocp4-cluster|openshift-cluster}-{GUID}-{UUID}` patterns (current + legacy cluster-name prefixes)
- **VolumeSnapshotContent Discovery:** Loads cluster-scoped VSCs to verify real ownership of parentless csi-snaps
- **PersistentVolume Discovery:** Loads cluster-scoped PVs to verify real ownership of parentless csi-vols
- **CSI Snapshot/Volume Handling:** Processes both via parent relationships
- **Trash Item Analysis:** Includes deleted/trashed items in discovery

### Performance Optimizations
- **CSI Snap Caching:** Prevents repeated parent lookups for CSI snapshots
- **Image List Caching:** Reuses expensive RBD list operations
- **Deferred Analysis:** Orders operations to minimize RBD API calls

### Analysis Features
- **Orphan Detection:** Identifies storage without corresponding active namespaces
- **Complexity Ordering:** Prioritizes cleanup by complexity (volumes → snapshots → trash)
- **Parentless Analysis:** Special handling for CSI snapshots without parents
- **Dependency Tracking:** Analyzes parent-child relationships

### Output Generation
- **Detailed Reporting:** Comprehensive analysis with actionable recommendations
- **Automated Scripts:** Generates ready-to-run cleanup scripts, live by default with a confirmation prompt
- **Safety Features:** `odf-cleanup.py` self-handles watchers/phantoms; manual-review logging for anything it still can't resolve; `odf-descendant-reaper.py`'s `CL_CLEANUP_LIST` mode re-verifies watchers/children fresh before removing any SAFE TO DELETE CSI leftover
- **Progress Tracking:** Statistics and status reporting throughout process

---

## Workflow Decisions

### GUID Extraction Decision

#### Decision Mechanisms:
- **Primary Pattern:** follows `VOLUME_GUID_PATTERN`
- **Secondary Pattern:** CSI snapshot parent lookup for indirect GUID discovery

#### Strategy:
- **Direct Extraction:** Use regex pattern matching for volume names
- **Parent Relationship:** For CSI snapshots, examine parent volume for GUID
- **Caching Logic:** Cache parent lookups to avoid repeated RBD API calls
- **Fallback Analysis:** Analyze parentless CSI snapshots separately

#### **Key Point:**
GUID extraction relies on **consistent naming patterns** established by OpenShift storage provisioning. The system handles both direct naming and parent relationships to ensure complete discovery coverage.

### CSI Snapshot Parent Relationship Decision

#### Decision Mechanisms:
- **Cache Check:** First check `csi_snap_guid_cache` for previous lookups
- **Parent Lookup:** Use RBD `parent_info()` to find parent volume
- **Parentless Detection:** Identify snapshots without parent relationships

#### Strategy:
- **Performance-First Approach:** Always check cache before expensive RBD operations
- **Comprehensive Analysis:** For parentless snapshots, analyze children and dependencies
- **Recommendation Engine:** Provide actionable recommendations for parentless snapshots

#### **Key Point:**
The caching strategy prevents **performance degradation** from repeated RBD API calls while ensuring comprehensive analysis of complex snapshot relationships.

### Orphan Complexity Ordering Decision

#### Decision Mechanisms:
- **Item Type Analysis:** Count volumes, snapshots, and trash items per GUID
- **Complexity Categorization:** Group by cleanup difficulty
- **Priority Assignment:** Order from simple to complex cleanup scenarios

#### Strategy:
- **Priority 1:** Volumes only (safest, simplest cleanup)
- **Priority 2:** Volumes + snapshots (moderate complexity)
- **Priority 3:** Volumes + snapshots + trash (highest complexity)
- **Safety-First Ordering:** Process simple cases first to minimize risk

#### **Key Point:**
Complexity ordering ensures **progressive risk management** by handling simple, low-risk orphans first before moving to complex scenarios that may require manual intervention.

### Cleanup Script Generation Decision

#### Decision Mechanisms:
- **Orphan Count Check:** Only generate script if orphans exist
- **Environment Replication:** Use current environment variables as template
- **Safety Integration:** Confirmation prompt before doing anything destructive

#### Strategy:
- **Template-Based Generation:** Create executable script with proper environment setup
- **Live By Default, Gated:** `DRY_RUN="false"` is the default, but the script always prompts for `y/n` confirmation before doing anything destructive
- **Single Tool Per GUID:** Just runs `odf-cleanup.py` - it now handles watcher checks and phantom entry cleanup itself, so no separate fallback tool is invoked automatically
- **Manual Review Logging:** Any GUID `odf-cleanup.py` can't resolve is appended to `needs_descendant_review.txt` for manual investigation with `odf-descendant-reaper.py`, instead of being retried automatically
- **Progressive Execution:** Process by priority levels with clear separation

#### **Key Point:**
Script generation defaults to actually completing the cleanup rather than just previewing it, but never proceeds without an explicit `y/n` confirmation, and never silently drops a GUID it couldn't resolve. `odf-descendant-reaper.py`'s GUID-based chain-walking (`analyze_guid()`) is intentionally kept out of the automated path - it remains a standalone manual diagnostic tool for cases `odf-cleanup.py` genuinely can't resolve on its own. Its separate `CL_CLEANUP_LIST` mode *is* wired into the generated script, but only for the parentless CSI leftovers this tool already verified via Kubernetes ownership - `odf-cleanup.py` is GUID-scoped and can't reach GUID-less images anyway.

### Parentless CSI Snapshot/Volume Analysis Decision

`csi-snap-*` and `csi-vol-*` are both handled the same way - only the Kubernetes object checked differs (`VolumeSnapshotContent` vs `PersistentVolume`). A descendant found via a resolvable RBD parent is never treated as parentless in the first place; this analysis only runs when the immediate parent lookup finds nothing.

#### Decision Mechanisms:
- **Child Discovery:** Use RBD `list_descendants()` to find children
- **GUID Analysis:** Extract GUIDs from child names
- **Active Status Check:** Compare child GUIDs against active namespace GUIDs
- **Kubernetes Ownership Check:** For the zero-children case, verify against real `VolumeSnapshotContent` (csi-snap) or `PersistentVolume` (csi-vol) objects instead of trusting the RBD-only signal
- **Recommendation Logic:** Provide action recommendations based on analysis

#### Strategy:
- **Child-Based Classification:** Determine safety based on child image activity first
- **KEEP Recommendation:** If any children belong to active namespaces
- **REVIEW Recommendation:** If children exist but are orphaned
- **Zero-Children Case Needs Verification:** Zero RBD children doesn't mean zero owners - a live `VolumeSnapshot`/PV can still reference this image with no clone ever made from it, so `_check_csi_snap_k8s_ownership()`/`_check_csi_vol_k8s_ownership()` decides the outcome:
  - **KEEP:** a matching VSC/PV exists and its namespace is still active
  - **REVIEW:** a matching VSC/PV exists but its namespace is orphaned (delete the VSC/PV too)
  - **REVIEW (fail-safe):** VSCs/PVs couldn't be loaded at all - can't verify, so don't risk it
  - **SAFE TO DELETE:** no RBD children and no matching VSC/PV reference found
- **ERROR Handling:** Graceful handling of analysis failures

#### **Key Point:**
Parentless analysis no longer treats "zero RBD children" as sufficient grounds for deletion - it cross-checks real Kubernetes ownership first (`VolumeSnapshotContent.snapshotHandle` for snapshots, `PersistentVolume.spec.csi.volumeHandle` for volumes - both verified to carry the same UUID as the RBD image name), since an image can still be referenced by a live k8s object without ever having been cloned.

### Empty Lab Verification Decision

#### Background:
`active_namespace_guids - odf_guids` can be non-trivial (e.g. dozens of GUIDs) - namespaces exist with no matching ODF volume at all. This could mean the lab genuinely never provisioned storage, or it could be a sign of a scanning gap.

#### Decision Mechanisms:
- **PVC Check:** List PVCs in each such GUID's namespace(s) via `CoreV1Api`
- **Zero PVCs:** Confirms the lab is genuinely empty - not just missed by the ODF-side scan
- **Any PVCs Found:** Flags for manual review instead of assuming empty - this pool is the only one the provisioner uses, so PVCs with no matching ODF GUID are unexpected and worth investigating directly

#### **Key Point:**
This check is about **confidence, not cleanup** - there's nothing to delete here (no ODF volumes exist for these GUIDs), it just confirms the "no footprint" GUIDs are legitimately empty rather than silently trusting a difference of two sets.