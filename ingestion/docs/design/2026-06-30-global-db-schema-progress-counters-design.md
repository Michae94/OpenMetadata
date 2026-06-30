# Global Database / DatabaseSchema progress counters, local Table tracking — Design

*OpenMetadata Python ingestion · 2026-06-30*

## Problem

We want live ingestion progress for DB connectors (Snowflake first) where:

- **Database** and **DatabaseSchema** counts are tracked **globally** — a run-level
  `done/total` for each that survives even after the underlying scope is pruned from the
  progress tree.
- **Table** counts are tracked **locally** — per active schema in the tree (`SALES.PUBLIC
  45/310`). Once a schema finishes, its node is **dropped from the tree** (pruned), but the
  monotonic overall ingested count is retained.

The current `ProgressRegistry` cannot express this. It has:

- A hierarchical tree (root → database → schema → table) that is **actively pruned**: when a
  schema's tables finish, `topology_runner.py:317` calls `progress.close(scope_path)` and the
  schema node disappears. Completed databases/schemas leave **no** run-level trace.
- A single global axis (`set_group`/`complete_group`/`group_progress`) used by PowerBI to render
  one `Workspaces 300/420` line. One axis cannot hold both `Database` and `DatabaseSchema`.

Commit `8928ed4ab1` previously deleted Snowflake's upfront database/schema counting
(`_push_progress_totals`, `_account_schema_names`, `_is_schema_filtered`, the
`SHOW TERSE SCHEMAS IN ACCOUNT` query) because the rollup header that consumed it was removed.
This design reinstates that counting, re-aimed at a new global-counter API.

## Decisions (from brainstorming)

1. **Totals known upfront (C).** Both `Database` and `DatabaseSchema` denominators are known
   before entity #1. The schema denominator requires an account-wide schema enumeration so it can
   be computed (and filtered) upfront — reinstating the listing `8928ed4ab1` removed.
2. **One keyed map of named global counters (A).** Replace the single `_group_*` axis with a
   `dict[entityType → GlobalCounter]`. `Database`, `DatabaseSchema`, and PowerBI's `Workspace` all
   become entries in the same map. One concept, one render path.
3. **Self-healing reconcile (B).** When the upfront schema total drifts from what is actually
   walked, the global total nudges toward the observed count at each reconcile point.

## Architecture

Three counting surfaces, each owned by the layer that can see it:

| Surface | Lives in | Survives pruning | For DB connectors |
|---|---|---|---|
| **Global counters** | `dict[type → GlobalCounter]` (flat, outside the tree) | **yes** | `Database X/N`, `DatabaseSchema Y/M` |
| **Local tree** | `ProgressNode` tree (pruned on scope close) | no — that's the point | active per-schema `Table` lines |
| **Monotonic total** | `_total_ingested` int (`assets_ingested()`) | yes | overall `Ingested: N assets` |

The tree (`open`/`advance`/`close`/`snapshot`), `assets_ingested()`, and the per-active-schema
`Table` rendering are **unchanged**. The new work is the global-counter map and the wiring that
feeds it.

## Component 1 — `ProgressRegistry` global counters

Replace the single `_group_label`/`_group_total`/`_group_done` trio with a keyed map.

```python
@dataclass
class GlobalCounter:
    total: Optional[int] = None              # declared denominator; None = running count only
    done: int = 0
    scope_estimates: Dict[str, int] = field(default_factory=dict)  # per-scope estimate, reconcilable totals
```

`self._global: Dict[str, GlobalCounter] = {}` keyed by entity type. All access stays under the
existing single `self._lock`.

### API (added / changed)

- `set_total(type, n)` — seed a flat global total. Snowflake `Database`; PowerBI `Workspace`.
- `seed_scope_total(type, scope, n)` — seed one scope's contribution to a **reconcilable** total
  and remember the estimate in `scope_estimates[scope]`. The counter's `total` is the running sum
  of estimates. Snowflake calls this per database for `DatabaseSchema`, so `total` reaches the full
  upfront value (e.g. 45) before the walk begins (decision C).
- `reconcile_scope_total(type, scope, observed)` — the self-healing path (decision B):
  ```
  counter.total += observed - counter.scope_estimates.get(scope, 0)
  counter.scope_estimates[scope] = observed
  counter.total = max(counter.total, counter.done)        # clamp: done never exceeds total
  ```
  Called when a scope's real, post-filter child list is materialized during the walk.
- `track(type)` — `counter.done += 1`. **No-op when `type` is not in `self._global`**, so
  connectors that never declared a global total get no phantom lines and the framework can call
  `track` unconditionally.
- `global_counters() -> list[tuple[str, int, Optional[int]]]` — snapshot accessor `(type, done,
  total)` per declared counter, insertion order, for the renderer and SSE.
- `is_reconcilable(type) -> bool` — true when `type` was seeded via `seed_scope_total` (i.e. has
  per-scope estimates). The topology runner uses this to decide whether to eager-materialize a
  container producer and reconcile it.
- **Delete** `set_group`, `complete_group`, `group_progress`.

`reconcile_scope_total` on a never-seeded scope behaves as a first declaration (estimate defaults
to 0), so a connector may rely purely on reconcile if it has no upfront per-scope estimate.

## Component 2 — Topology runner wiring

Two additions, both at spots that already exist; gated on `progress_tracking_enabled`.

1. **Count `done` at scope close.** Where the single-thread path closes a finished container
   (`topology_runner.py:317-318`) and the multithread path does the same (`:393-394`), add
   `progress.track(entity_type_name)` alongside the existing `progress.close(scope_path)`. Closing a
   schema node bumps `DatabaseSchema.done`; closing a database node bumps `Database.done`. `track`
   is a no-op for types nobody declared, so non-DB connectors are unaffected.

2. **Reconcile when a container's real child list is known.** Processing the `DatabaseSchema`
   node materializes a database's post-filter schema list; that list's length is the real schema
   count for **that database**. Call `progress.reconcile_scope_total("DatabaseSchema", <db>,
   len(schema_list))`. This requires the `DatabaseSchema` (container) producer to materialize
   eagerly under progress tracking — today only leaves do, at `:296-298`; containers stay lazy at
   `:300-302`. Gate the eager materialization on a registry predicate
   `progress.is_reconcilable(entity_type_name)` (true only for types seeded via `seed_scope_total`,
   i.e. `DatabaseSchema`) — every other producer keeps iterating lazily, preserving the PowerBI
   `state.enter`/`finally: state.exit` contract called out at `:287-289`.

**Scope-key convention (correctness-critical).** The reconcile and the upfront seed must key on the
**same** scope: the database. For the `DatabaseSchema` node the database is the node's
`parent_path` (the runner already computes `current_progress_path("DatabaseSchema")` → `[db]`), so
the scope key is that parent path's single element `db` — the same `db` Component 3 passes to
`seed_scope_total`. (Note this is the node's *parent* path, not `_scope_path_for_node`, which would
be the schema's own `[db, schema]`.) `track`/`done` is the orthogonal axis and keys on nothing — it
just increments the type's counter at close.

## Component 3 — Snowflake upfront declaration

Reinstate, re-aimed at the global API, what `8928ed4ab1` deleted. In `get_database_names()` (first
producer, single-threaded, before workers spawn — the same place the old `_push_progress_totals`
ran):

- Restore the account-wide schema listing: `SNOWFLAKE_GET_SCHEMATA = "SHOW TERSE SCHEMAS IN
  ACCOUNT"` (capped at 10k rows; per-database enumeration fallback on privilege/availability error,
  logged once) returning `{database: [schema_names]}`.
- Restore `_is_schema_filtered(database, schema)` — applies `schemaFilterPattern` the same way the
  lazy producer does (FQN vs bare name per `useFqnForFiltering`), context-free.
- Apply `databaseFilterPattern` then `schemaFilterPattern` and declare:
  - `progress.set_total("Database", len(filtered_databases))`
  - per database: `progress.seed_scope_total("DatabaseSchema", db, len(filtered_schemas[db]))`

The walk then drives reconcile + `done` generically via Component 2. If the account-wide SHOW is
unavailable, totals fall back to per-database enumeration; if that also fails for a database, that
database simply contributes no upfront schema estimate and reconcile seeds it from the walk.

## Component 4 — PowerBI migration

Mechanical; the `dashboard_service.py` helper layer absorbs most of it so the 5 call-sites in
`powerbi/metadata.py` keep their helper names.

- `_open_group_progress` → `progress.set_total("Workspace", n)`
- `_close_group_progress` → `progress.track("Workspace")`
- `_advance_group_progress` → drop the old group call. `Dashboard`/`Chart`/`DashboardDataModel`
  remain bare `assets_ingested` counts (no run denominator), exactly as before and as the unified
  design specifies for assembling types.

## Component 5 — Rendering & SSE

**CLI** (`progress_render.py`): `_header()` iterates `global_counters()` and emits one line per
declared counter — `Database done/total` (or bare `done` when `total is None`) — followed by the
existing `Ingested: N assets`. The active-scope tree (per-schema `Table` lines) and `payload()`
(bare `progressNode` tree, `processed` = `assets_ingested()`) are unchanged.

Snowflake render target:

```
Database         2/4
DatabaseSchema  12/45
Ingested: 1,204 assets
SALES.PUBLIC   Table 45/310
SALES.STAGING  Table  8/120
```

**SSE schema** (`progressUpdate.json` + `make generate`): the single-group fields are brand-new on
this unreleased branch, so replace them rather than keep both. Remove `groupLabel`, `groupDone`,
`groupTotal`; add:

```json
"globalCounters": {
  "description": "Run-level counters that survive scope pruning (e.g. Database, DatabaseSchema, Workspace). Each carries done and an optional upfront total.",
  "type": "array",
  "items": {
    "type": "object",
    "properties": {
      "entityType": { "type": "string" },
      "done": { "type": "integer" },
      "total": { "type": ["integer", "null"] }
    },
    "additionalProperties": false
  }
}
```

`workflow_status_mixin.py:204` maps `reporter.global_counters()` into `globalCounters`;
`reporter.group()` and its `groupLabel/Done/Total` mapping are removed. Check the UI for any
`groupLabel`/`groupTotal` consumer and update it to read `globalCounters`.

## Honesty invariant

A `total` is only ever a number the connector deliberately committed to (upfront seed) or corrected
toward observation (reconcile). `done` never renders above `total` (clamp). A counter with no
declared total renders a bare running count, never a faked denominator.

## Testing

- **Registry unit**: `seed_scope_total` sums to the upfront total; `reconcile_scope_total` applies
  the delta and clamps; `track` increments only declared types and is a no-op otherwise;
  `global_counters()` ordering; `set_group`/`complete_group`/`group_progress` removal.
- **Topology runner**: closing a schema node bumps `DatabaseSchema.done`; closing a database node
  bumps `Database.done`; reconcile nudges `DatabaseSchema.total` toward the walked count; lazy
  iteration is preserved for non-reconcilable child types.
- **Snowflake**: `get_database_names()` seeds `Database` and `DatabaseSchema` totals from
  post-filter counts (replaces the test `8928ed4ab1` gutted); account-wide SHOW failure falls back
  to per-database enumeration without crashing.
- **PowerBI**: `Workspace` counter seeds and increments through the keyed API; dashboards/charts
  stay bare counts.

Tests use pytest with plain `assert` (no `unittest.TestCase`), per repo Python standards.

## Out of scope

- ETA / throughput estimation (the unified design's run-level ETA) — not requested here.
- The end-of-run outcome/summary feeds (sink `OutcomeStore`, `IssueStore`) — separate work.
- Adopting global totals in other DB connectors (Postgres/MySQL/MSSQL) — the registry + runner
  changes are generic and ready, but only Snowflake declares totals in this change.

## Files touched

- `ingestion/src/metadata/utils/progress_registry.py` — global-counter map + API.
- `ingestion/src/metadata/ingestion/api/topology_runner.py` — `track` at close, reconcile at
  schema materialization, gated eager materialization.
- `ingestion/src/metadata/ingestion/source/database/snowflake/metadata.py` + `queries.py` —
  reinstated schema enumeration, filter helper, upfront declaration.
- `ingestion/src/metadata/ingestion/source/dashboard/dashboard_service.py` — group helpers → keyed
  API; PowerBI call-sites unchanged.
- `ingestion/src/metadata/workflow/progress_render.py` — multi-counter header; `group()` removed.
- `ingestion/src/metadata/workflow/workflow_status_mixin.py` — map `global_counters()` to SSE.
- `openmetadata-spec/.../ingestionPipelines/progressUpdate.json` — `globalCounters` array (+ `make
  generate`).
- UI consumer of `groupLabel`/`groupTotal`, if any.
- Tests across registry, runner, Snowflake, PowerBI.
</content>
</invoke>
