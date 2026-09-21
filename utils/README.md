# ODF Cleanup Utilities

Supporting tools for ODF cleanup operations - discovery and monitoring utilities that complement the main cleanup functionality.

## Tools Overview

- **odf-oc-compare.py** - Discovery tool that identifies orphaned storage by comparing OpenShift namespaces with ODF volumes
- **odf-descendant-reaper.py** - Discovers and classifies stranded RBD descendant chains blocking `odf-cleanup.py`, with an optional execute mode
- **odf-cleanup-monitor.py** - Monitoring tool that analyzes cleanup job failures and generates reports

---

## ODF-OpenShift Comparison Tool

The `odf-oc-compare.py` script compares active OpenShift namespaces with ODF RBD images to identify orphaned lab GUIDs. This helps discover storage resources that are no longer associated with active labs and can be safely cleaned up.

### Key Features

- **Namespace Analysis**: Discovers active lab GUIDs from OpenShift projects (pattern: `sandbox-{GUID}-*`)
- **ODF Resource Discovery**: Analyzes volumes, CSI snapshots, and trash items
- **Parentless CSI Snapshot Analysis**: Identifies potential boot/base images by analyzing children relationships
- **Empty Lab Verification**: For active namespaces with zero ODF footprint, checks real OCP PVCs to confirm they're genuinely empty rather than a scanning gap
- **RBD Namespace Awareness**: Scans every RBD namespace in the pool (Ceph multi-tenancy, distinct from k8s namespaces), not just the default one - some provisioners isolate a lab's images this way; orphans found there go through the same `odf-cleanup.py` (with `CL_RBD_NAMESPACE` set), which also removes the namespace itself once empty. Namespaces found completely empty from the start (no GUID ever lived there) are flagged too (unless their own embedded GUID still matches an active OCP namespace) and routed straight to the reaper for removal
- **Smart Ordering**: Prioritizes cleanup by complexity (volumes only → volumes+snapshots → volumes+snapshots+trash)
- **Automated Script Generation**: Creates ready-to-run cleanup scripts

### Workflow

The comparison tool follows this logical workflow:

```
Workflow Step                     Implementation
-------------                     --------------
compare                       →   run_comparison()
get projects                  →   discover_namespace_guids()  
get csi-snaps + analyze       →   discover_odf_guids() + _analyze_parentless_csi_snap()
get volumes                   →   _extract_guid_from_image()
compare guids                 →   compare_and_find_orphans()
order for deletion            →   _order_guids_by_complexity()
create script                 →   generate_cleanup_script()
run cleanup                   →   Generated bash script
```

### Usage

```console
cd odf-cleanup
source env.sh
python3 utils/odf-oc-compare.py
```

Can also be run from inside `utils/` directly (`cd utils && python3 odf-oc-compare.py`) - the generated cleanup script auto-detects the right relative path to `odf-cleanup.py` either way.


### Output

The script generates:
1. **Detailed comparison report** showing active vs orphaned GUIDs
2. **Parentless CSI snapshot analysis** with safety recommendations  
3. **Automated cleanup script** (`cleanup_orphaned_guids.sh`) ordered by complexity

---

## ODF Descendant Reaper

The `odf-descendant-reaper.py` script discovers and classifies stranded RBD descendant chains that block `odf-cleanup.py` with a "still has active descendants" error. It rebuilds the true parent/child tree for a GUID (or a specific volume), classifies each chain, and can remove what's safe directly via the RBD Python bindings.

### Key Features

- **Descendant Chain Analysis** - Rebuilds the real parent/child tree via `parent_info()`, not a flat `list_descendants()` dump
- **Classification** - `SAFE_TO_REMOVE` (zero watchers throughout, terminal chain), `NEEDS_REVIEW` (active watchers or depth limit hit), or `ERROR` (volume couldn't be opened)
- **Phantom Entry Detection** - Catches the case where `rbd_id.<name>` exists but its `rbd_header.<id>` is missing, and can clean up the dangling pointer
- **Execute Mode** - `DRY_RUN=false` removes `SAFE_TO_REMOVE` chains, confirmed phantom entries, and the volume itself once safe (including the "clean orphan" case of zero descendants to begin with) - same convention as `odf-cleanup.py`
- **RBD Namespace Targeting** - Optional `CL_RBD_NAMESPACE` scopes the whole run to a named RBD namespace (Ceph multi-tenancy, distinct from k8s namespaces) instead of the pool's default; removes the namespace itself once it's empty, and can be set alone to just remove an already-empty one

### Usage

```console
cd odf-cleanup
source env.sh
export CL_LAB="your-guid"      # or CL_VOLUME="specific-image-name"
export DRY_RUN="true"          # false to actually remove what's classified safe
python3 utils/odf-descendant-reaper.py
```

### Output

- Per-chain breakdown with watcher/timestamp/snapshot detail and classification
- Manual removal order (leaf-first `rbd snap unprotect`/`rbd snap rm`/`rbd rm`) for review
- In execute mode, performs the removals directly instead of just printing them

---

## ODF Cleanup Monitor

The `odf-cleanup-monitor.py` script monitors ODF cleanup jobs in OpenShift and reports failures. It analyzes job logs to identify failed cleanup operations and generates reports for manual intervention.

### Key Features

- **Job Monitoring** - Scans cleanup namespace for failed jobs
- **Log Analysis** - Extracts error details and LAB GUIDs from job logs
- **Dual Reporting** - Console summaries and CSV reports
- **Error Classification** - Categorizes failures by type (ERROR, FAILED, WARNING)

### Usage

```console
# Monitor default cleanup namespace
python3 utils/odf-cleanup-monitor.py

# Monitor different namespace with debug
python3 utils/odf-cleanup-monitor.py --namespace my-cleanup --debug

# Generate CSV report only
python3 utils/odf-cleanup-monitor.py --format csv --csv failures.csv
```

### Output

- **Console**: Summary with success/failure counts and error details
- **CSV**: Structured data (job_name, guid, status, error_type, error_reason)
- **Exit codes**: 0 for no failures, 1 if cleanup failures detected

---

## Requirements

### Python Packages
```console
pip install kubernetes rados rbd
```

### Access Requirements
- **ODF Cluster**: Configuration file and keyring (for comparison tool and descendant reaper)
- **OpenShift/Kubernetes**: Valid kubeconfig or in-cluster service account
- **Namespace Access**: Read permissions for target namespaces and cleanup jobs

### Environment Variables
- **Comparison tool**: `CL_POOL`, `CL_CONF`, `CL_KEYRING` (CL_LAB not required)
- **Descendant reaper**: `CL_POOL`, `CL_CONF`, `CL_KEYRING`, and either `CL_LAB` or `CL_VOLUME`; optional `MAX_CHAIN_DEPTH` (default: 10), `DRY_RUN` (default: "true")
- **Monitor tool**: None required (uses kubeconfig/service account)

---

## Documentation

For detailed information:
- [Complete comparison tool documentation](../Documentation/odf-oc-compare.md)
- [Complete monitoring tool documentation](../Documentation/odf-cleanup-monitor.md)