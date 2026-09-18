# ODF-OpenShift Comparator Script Documentation

## Overview
The `odf-oc-compare.py` script compares OpenShift namespaces with ODF (OpenShift Data Foundation) RBD images to identify orphaned lab GUIDs. It discovers active lab environments from OpenShift and compares them with ODF storage volumes to find orphaned storage that can be safely cleaned up.

## Execution Flow
```
main() → OdfOpenShiftComparator.run_comparison() → connect_odf() → discover_namespace_guids() → discover_volume_snapshot_contents() → discover_odf_guids() → compare_and_find_orphans() → generate_report() → generate_cleanup_script()
```

### High-Level Execution Flow Diagram

```mermaid
graph TD
    A["Connect to ODF Cluster"] --> B["Discover Namespace GUIDs"]
    B --> B2["Discover VolumeSnapshotContents (non-fatal)"]
    B2 --> C["Discover ODF GUIDs"]
    C --> D["Compare & Find Orphans"]
    D --> E["Generate Report"]
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
- `csi_snap_guid_cache` - Cache for CSI snapshot parent lookups
- `parentless_csi_snaps` - Analysis of CSI snapshots without parents
- `volume_snapshot_contents` - Cluster's `VolumeSnapshotContent` objects, used to verify real ownership of parentless csi-snaps; `None` means "couldn't load" (callers fail safe into REVIEW), `[]` means "loaded, none exist"

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

#### `discover_odf_guids()`
**When:** After VolumeSnapshotContent discovery
**Purpose:** Discovers all lab GUIDs from ODF RBD images and snapshots
**Does:**
- Lists all active RBD images in the pool
- Lists all trash items in the pool
- Processes each image through `_extract_guid_from_image()`
- Handles special case of CSI snapshots via parent lookups
- Caches results for performance optimization
- Updates statistics for ODF discovery

#### Helper Methods for Discovery:
- `_extract_guid_from_image()` - Extracts GUID from image name using regex patterns
- `_get_guid_from_csi_snap_parent()` - Gets GUID from CSI snapshot's parent (with caching)
- `_analyze_parentless_csi_snap()` - Analyzes CSI snapshots that have no parent
- `_check_csi_snap_k8s_ownership()` - For a parentless csi-snap with zero RBD children, matches its UUID against loaded `VolumeSnapshotContent` `snapshotHandle`s to check whether it's still referenced (and whether that reference's namespace is active)
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

### Phase 4: Reporting

#### `generate_report()`
**When:** After orphan analysis
**Purpose:** Generates comprehensive comparison report
**Does:**
- Reports orphaned GUIDs ordered by cleanup complexity
- Analyzes parentless CSI snapshots with recommendations
- Provides detailed statistics summary
- Categorizes findings for actionable insights

#### `generate_cleanup_script()`
**When:** After report generation
**Purpose:** Creates automated bash script for orphan cleanup
**Does:**
- Generates executable shell script with environment setup
- Orders GUIDs by cleanup priority (simple → complex)
- Defaults `DRY_RUN="false"` (live) but prompts for `y/n` confirmation before running anything
- Per GUID, runs `odf-cleanup.py` (which now self-handles watcher checks, phantom entries, and flattening foreign/cross-namespace descendants); on failure it's logged to `needs_descendant_review.txt` for manual investigation with `odf-descendant-reaper.py` instead of being retried automatically

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
- **CSI Snapshot Handling:** Processes snapshots via parent relationships
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
- **Safety Features:** `odf-cleanup.py` self-handles watchers/phantoms/foreign descendants; manual-review logging for anything it still can't resolve
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
- **Single Tool Per GUID:** Just runs `odf-cleanup.py` - it now handles watcher checks, phantom entry cleanup, and flattening foreign (cross-namespace) descendants itself, so no separate fallback tool is invoked automatically
- **Manual Review Logging:** Any GUID `odf-cleanup.py` can't resolve is appended to `needs_descendant_review.txt` for manual investigation with `odf-descendant-reaper.py`, instead of being retried automatically
- **Progressive Execution:** Process by priority levels with clear separation

#### **Key Point:**
Script generation defaults to actually completing the cleanup rather than just previewing it, but never proceeds without an explicit `y/n` confirmation, and never silently drops a GUID it couldn't resolve. `odf-descendant-reaper.py` is intentionally kept out of the automated path now - it remains a standalone manual diagnostic tool for the cases `odf-cleanup.py` genuinely can't resolve on its own (e.g. a watched descendant that IS part of the target GUID).

### Parentless CSI Snapshot Analysis Decision

#### Decision Mechanisms:
- **Child Discovery:** Use RBD `list_descendants()` to find children
- **GUID Analysis:** Extract GUIDs from child names
- **Active Status Check:** Compare child GUIDs against active namespace GUIDs
- **Kubernetes Ownership Check:** For the zero-children case, verify against real `VolumeSnapshotContent` objects instead of trusting the RBD-only signal
- **Recommendation Logic:** Provide action recommendations based on analysis

#### Strategy:
- **Child-Based Classification:** Determine safety based on child image activity first
- **KEEP Recommendation:** If any children belong to active namespaces
- **REVIEW Recommendation:** If children exist but are orphaned
- **Zero-Children Case Needs Verification:** Zero RBD children doesn't mean zero owners - a `VolumeSnapshot` can still reference this csi-snap with no clone ever made from it, so `_check_csi_snap_k8s_ownership()` decides the outcome:
  - **KEEP:** a matching VolumeSnapshotContent exists and its namespace is still active
  - **REVIEW:** a matching VolumeSnapshotContent exists but its namespace is orphaned (delete the VSC too)
  - **REVIEW (fail-safe):** VolumeSnapshotContents couldn't be loaded at all - can't verify, so don't risk it
  - **SAFE TO DELETE:** no RBD children and no VolumeSnapshotContent reference found
- **ERROR Handling:** Graceful handling of analysis failures

#### **Key Point:**
Parentless snapshot analysis no longer treats "zero RBD children" as sufficient grounds for deletion - it cross-checks real Kubernetes ownership first, since a snapshot can still be referenced by a live `VolumeSnapshot`/`VolumeSnapshotContent` without ever having been cloned.