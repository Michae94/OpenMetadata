# Global DB/Schema Progress Counters Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Track Database and DatabaseSchema counts as run-level global counters that survive tree pruning, while keeping Table counts local per active schema, for Snowflake (with a generic framework other DB connectors can adopt).

**Architecture:** Replace `ProgressRegistry`'s single group axis with a keyed map of `GlobalCounter`s living outside the tree. The topology runner increments a counter's `done` when it prunes a finished container, and reconciles a reconcilable counter's `total` when a container's real child list materializes. Snowflake declares `Database`/`DatabaseSchema` totals upfront from an account-wide schema listing; PowerBI's `Workspace` axis migrates to the same keyed API. The SSE `ProgressUpdate` carries a `globalCounters` array.

**Tech Stack:** Python 3.10–3.11, Pydantic 2.x, pytest, JSON Schema → datamodel-codegen via `make generate`.

## Global Constraints

- Python: pytest with plain `assert`; no `unittest.TestCase`; classes prefixed `Test`; `unittest.mock` allowed for boundaries.
- Run Python via the repo venv (`source env/bin/activate`); never reinstall the venv.
- Connector-specific logic stays in connector files (Snowflake counting in `snowflake/metadata.py`, not shared utils).
- All caches bounded — not relevant here (counters are O(types)+O(scopes), names never retained beyond the declaring call).
- `make generate` regenerates Pydantic + Java models after any JSON-schema change; run it before tests that import the model.
- Format with `make py_format` before each commit touching Python.

---

### Task 1: Registry global-counter API

**Files:**
- Modify: `ingestion/src/metadata/utils/progress_registry.py` (`__init__` at :68-75; add methods; delete `set_group`/`complete_group`/`group_progress` at :98-118)
- Test: `ingestion/tests/unit/test_progress_registry.py` (replace the group tests at :282-315)

**Interfaces:**
- Produces:
  - `GlobalCounter` dataclass: `total: Optional[int]`, `done: int`, `scope_estimates: Dict[str,int]`, `reconcilable: bool`.
  - `set_total(type_: str, total: Optional[int]) -> None`
  - `set_reconcilable(type_: str) -> None`
  - `seed_scope_total(type_: str, scope: str, n: int) -> None`
  - `reconcile_scope_total(type_: str, scope: str, observed: int) -> None`
  - `track(type_: str) -> None`
  - `is_reconcilable(type_: str) -> bool`
  - `global_counters() -> List[Tuple[str, int, Optional[int]]]`

- [ ] **Step 1: Write the failing tests** — replace lines 282-315 of `tests/unit/test_progress_registry.py` (the `test_group_progress_*` / `test_set_group_*` / `test_complete_group_*` block) with:

```python
    def test_global_counters_empty_by_default(self):
        reg = ProgressRegistry()
        assert reg.global_counters() == []

    def test_set_total_then_track(self):
        reg = ProgressRegistry()
        reg.set_total("Database", 4)
        assert reg.global_counters() == [("Database", 0, 4)]
        reg.track("Database")
        reg.track("Database")
        assert reg.global_counters() == [("Database", 2, 4)]

    def test_set_total_with_unknown_total(self):
        reg = ProgressRegistry()
        reg.set_total("Workspaces", None)
        reg.track("Workspaces")
        assert reg.global_counters() == [("Workspaces", 1, None)]

    def test_track_unknown_type_is_noop(self):
        reg = ProgressRegistry()
        reg.track("Nope")
        assert reg.global_counters() == []

    def test_track_does_not_touch_asset_counter(self):
        reg = ProgressRegistry()
        reg.advance([], "Table")
        reg.set_total("Database", 3)
        reg.track("Database")
        assert reg.assets_ingested() == 1
        assert reg.global_counters() == [("Database", 1, 3)]

    def test_seed_scope_total_sums_and_is_reconcilable(self):
        reg = ProgressRegistry()
        reg.seed_scope_total("DatabaseSchema", "db1", 10)
        reg.seed_scope_total("DatabaseSchema", "db2", 35)
        assert reg.global_counters() == [("DatabaseSchema", 0, 45)]
        assert reg.is_reconcilable("DatabaseSchema") is True
        assert reg.is_reconcilable("Database") is False

    def test_reconcile_scope_total_applies_delta(self):
        reg = ProgressRegistry()
        reg.seed_scope_total("DatabaseSchema", "db1", 10)
        reg.seed_scope_total("DatabaseSchema", "db2", 35)
        reg.reconcile_scope_total("DatabaseSchema", "db1", 12)
        assert reg.global_counters() == [("DatabaseSchema", 0, 47)]

    def test_reconcile_unknown_type_is_noop(self):
        reg = ProgressRegistry()
        reg.reconcile_scope_total("DatabaseSchema", "db1", 5)
        assert reg.global_counters() == []

    def test_total_never_below_done(self):
        reg = ProgressRegistry()
        reg.seed_scope_total("DatabaseSchema", "db1", 1)
        reg.track("DatabaseSchema")
        reg.track("DatabaseSchema")
        assert reg.global_counters() == [("DatabaseSchema", 2, 2)]

    def test_set_reconcilable_creates_counter(self):
        reg = ProgressRegistry()
        reg.set_reconcilable("DatabaseSchema")
        assert reg.is_reconcilable("DatabaseSchema") is True
        reg.reconcile_scope_total("DatabaseSchema", "db1", 7)
        assert reg.global_counters() == [("DatabaseSchema", 0, 7)]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `source env/bin/activate && cd ingestion && python -m pytest tests/unit/test_progress_registry.py -k "global_counters or set_total or track or seed_scope or reconcile or set_reconcilable or total_never" -v`
Expected: FAIL — `AttributeError: 'ProgressRegistry' object has no attribute 'set_total'`.

- [ ] **Step 3: Add the `GlobalCounter` dataclass** — in `progress_registry.py`, after the `ProgressNodeSnapshot` class (after line 61), add:

```python
@dataclass
class GlobalCounter:
    """A run-level counter that lives outside the progress tree, so pruning a
    completed scope never erases it. ``total`` is the declared denominator (None
    = running count only). ``scope_estimates`` holds each scope's last-known
    contribution so the total can be reconciled by delta. ``reconcilable`` marks
    a counter whose total the framework may nudge toward observed counts."""

    total: Optional[int] = None  # noqa: UP045
    done: int = 0
    scope_estimates: Dict[str, int] = field(default_factory=dict)  # noqa: UP006
    reconcilable: bool = False
```

- [ ] **Step 4: Swap the group fields for the keyed map** — replace lines 71-73 of `__init__`:

```python
        self._group_label: Optional[str] = None  # noqa: UP045
        self._group_total: Optional[int] = None  # noqa: UP045
        self._group_done: int = 0
```

with:

```python
        self._global: Dict[str, GlobalCounter] = {}  # noqa: UP006
```

- [ ] **Step 5: Delete the old group methods and add the keyed API** — replace the `set_group`/`complete_group`/`group_progress` block (lines 98-118) with:

```python
    def set_total(self, type_: str, total: Optional[int]) -> None:  # noqa: UP045
        """Declare a flat global total for ``type_`` (e.g. ``Database`` = 4).
        Header-level and independent of the tree — survives pruning."""
        with self._lock:
            self._global.setdefault(type_, GlobalCounter()).total = total

    def set_reconcilable(self, type_: str) -> None:
        """Mark ``type_`` as a reconcilable global counter without seeding a
        total — the framework will build the total from observed scope counts."""
        with self._lock:
            self._global.setdefault(type_, GlobalCounter()).reconcilable = True

    def seed_scope_total(self, type_: str, scope: str, n: int) -> None:
        """Seed one scope's contribution to ``type_``'s total upfront (and mark
        it reconcilable). The total is the running sum of all seeded scopes."""
        with self._lock:
            self._apply_scope_total(type_, scope, n)

    def reconcile_scope_total(self, type_: str, scope: str, observed: int) -> None:
        """Nudge ``type_``'s total toward the real ``observed`` count for
        ``scope``. No-op when ``type_`` was never declared."""
        with self._lock:
            if type_ in self._global:
                self._apply_scope_total(type_, scope, observed)

    def _apply_scope_total(self, type_: str, scope: str, n: int) -> None:
        counter = self._global.setdefault(type_, GlobalCounter())
        counter.reconcilable = True
        previous = counter.scope_estimates.get(scope, 0)
        counter.total = (counter.total or 0) + n - previous
        counter.scope_estimates[scope] = n
        if counter.total < counter.done:
            counter.total = counter.done

    def track(self, type_: str) -> None:
        """Record one completed scope of ``type_``. No-op for an undeclared
        type, so the framework may call it unconditionally at scope close."""
        with self._lock:
            counter = self._global.get(type_)
            if counter is not None:
                counter.done += 1
                if counter.total is not None and counter.total < counter.done:
                    counter.total = counter.done

    def is_reconcilable(self, type_: str) -> bool:
        with self._lock:
            counter = self._global.get(type_)
            return counter is not None and counter.reconcilable

    def global_counters(self) -> "List[Tuple[str, int, Optional[int]]]":  # noqa: UP006,UP045
        """``(type, done, total)`` per declared global counter, insertion order."""
        with self._lock:
            return [(type_, c.done, c.total) for type_, c in self._global.items()]
```

- [ ] **Step 6: Update the module docstring note** — in the docstring at lines 22-24, the sentence "There is no global total and no ETA" is now inaccurate. Replace that final paragraph (lines 22-24) with:

```python
Leaf ``processed`` is authoritative; container progress is *derived* at
``snapshot()`` time (a container is complete when all its children are).
Run-level ``GlobalCounter``s (declared via ``set_total``/``seed_scope_total``)
live outside the tree and survive pruning; the tree itself carries no global
total and no ETA.
```

- [ ] **Step 7: Run the tests to verify they pass**

Run: `source env/bin/activate && cd ingestion && python -m pytest tests/unit/test_progress_registry.py -v`
Expected: PASS (all, including the pre-existing tree/asset tests).

- [ ] **Step 8: Format and commit**

```bash
cd ingestion && make py_format
git add src/metadata/utils/progress_registry.py tests/unit/test_progress_registry.py
git commit -m "feat(ingestion): keyed global counters on ProgressRegistry (replace single group axis)"
```

---

### Task 2: Multi-counter CLI header + reporter

**Files:**
- Modify: `ingestion/src/metadata/workflow/progress_render.py` (`cli` :62-67, `_header` :69-75, `group` :77-85)
- Test: `ingestion/tests/unit/workflow/test_progress_rendering.py` (update header/group tests :139-182)

**Interfaces:**
- Consumes: `ProgressRegistry.global_counters()` (Task 1).
- Produces: `ProgressReporter.global_counters() -> List[Tuple[str, int, Optional[int]]]` (passthrough for SSE, replaces `group()`); header renders one line per counter then `Ingested: N assets`.

- [ ] **Step 1: Update the rendering tests** — in `tests/unit/workflow/test_progress_rendering.py`, replace the four group-based tests (`test_header_shows_group_and_assets` :139, `test_header_with_unknown_total_on_empty_tree` :150, `test_header_renders_with_empty_tree_when_group_active` :156, and the `group()` test around :178) with:

```python
    def test_header_shows_counters_then_assets(self):
        reg = ProgressRegistry()
        reg.set_total("Database", 4)
        reg.seed_scope_total("DatabaseSchema", "db1", 45)
        reg.track("Database")
        reg.track("Database")
        for _ in range(12):
            reg.track("DatabaseSchema")
        reg.advance([], "Table")
        lines = ProgressReporter(reg).cli().splitlines()
        assert lines[0] == "Database 2/4"
        assert lines[1] == "DatabaseSchema 12/45"
        assert lines[2] == "Ingested: 1 assets"

    def test_header_unknown_total_renders_bare_count(self):
        reg = ProgressRegistry()
        reg.set_total("Workspaces", None)
        reg.track("Workspaces")
        assert ProgressReporter(reg).cli() == "Workspaces 1\nIngested: 0 assets"

    def test_header_renders_with_empty_tree_when_counter_active(self):
        reg = ProgressRegistry()
        reg.set_total("Workspaces", 4)
        for _ in range(4):
            reg.track("Workspaces")
        assert ProgressReporter(reg).cli() == "Workspaces 4/4\nIngested: 0 assets"

    def test_reporter_global_counters_passthrough(self):
        reg = ProgressRegistry()
        reg.set_total("Workspaces", 10)
        reg.track("Workspaces")
        reg.track("Workspaces")
        reg.track("Workspaces")
        assert ProgressReporter(reg).global_counters() == [("Workspaces", 3, 10)]
```

Keep `test_no_group_keeps_legacy_header` (:163) and `test_cli_empty_when_no_progress` (:172) but rename the former's body assertion target if needed — it asserts `splitlines()[0] == "Ingested: 1 assets"`, which still holds. The empty-tree test (:172) still asserts `cli() == ""`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `source env/bin/activate && cd ingestion && python -m pytest tests/unit/workflow/test_progress_rendering.py -v`
Expected: FAIL — `AttributeError: 'ProgressRegistry' object has no attribute 'set_group'` (old tests) and `'ProgressReporter' object has no attribute 'global_counters'`.

- [ ] **Step 3: Rewrite `cli`, `_header`, and replace `group` with `global_counters`** — replace lines 62-85 of `progress_render.py`:

```python
    def cli(self) -> str:
        snapshot = self._registry.snapshot()
        counters = self._registry.global_counters()
        header = self._header(counters)
        if snapshot is None:
            return header if counters else ""
        lines: List[str] = []  # noqa: UP006
        _render_joined(snapshot, [], lines)
        tree = "\n".join(lines)
        return f"{header}\n{tree}" if tree else header

    def _header(self, counters: "List[Tuple[str, int, Optional[int]]]") -> str:  # noqa: UP006,UP045
        lines: List[str] = []  # noqa: UP006
        for type_, done, total in counters:
            lines.append(f"{type_} {done}/{total}" if total is not None else f"{type_} {done}")
        lines.append(f"Ingested: {self._registry.assets_ingested():,} assets")
        return "\n".join(lines)

    def global_counters(self) -> "List[Tuple[str, int, Optional[int]]]":  # noqa: UP006,UP045
        """Run-level counters ``(type, done, total)`` for the SSE
        ``ProgressUpdate.globalCounters``. Independent of the progress tree, so
        reported even when the active tree is momentarily empty."""
        return self._registry.global_counters()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `source env/bin/activate && cd ingestion && python -m pytest tests/unit/workflow/test_progress_rendering.py -v`
Expected: PASS.

- [ ] **Step 5: Format and commit**

```bash
cd ingestion && make py_format
git add src/metadata/workflow/progress_render.py tests/unit/workflow/test_progress_rendering.py
git commit -m "feat(ingestion): multi-counter progress header; reporter.global_counters()"
```

---

### Task 3: SSE schema — `globalCounters` array

**Files:**
- Modify: `openmetadata-spec/src/main/resources/json/schema/entity/services/ingestionPipelines/progressUpdate.json` (remove `groupLabel`/`groupDone`/`groupTotal`; add `globalCounters`)
- Modify: `ingestion/src/metadata/workflow/workflow_status_mixin.py` (:203-211, the `ProgressUpdate(...)` construction)
- Test: `ingestion/tests/unit/workflow/test_progress_rendering.py` (the SSE-shaped test around :189-214)

**Interfaces:**
- Consumes: `ProgressReporter.global_counters()` (Task 2).
- Produces: `ProgressUpdate.globalCounters: List[{entityType, done, total}]`.

- [ ] **Step 1: Edit the JSON schema** — in `progressUpdate.json`, delete the `groupLabel`, `groupDone`, and `groupTotal` properties and add under `properties`:

```json
    "globalCounters": {
      "description": "Run-level counters that survive scope pruning (e.g. Database, DatabaseSchema, Workspace). Each carries done and an optional upfront total.",
      "type": "array",
      "items": {
        "type": "object",
        "javaType": "org.openmetadata.schema.entity.services.ingestionPipelines.GlobalCounter",
        "properties": {
          "entityType": { "type": "string" },
          "done": { "type": "integer" },
          "total": { "type": ["integer", "null"] }
        },
        "additionalProperties": false
      }
    },
```

- [ ] **Step 2: Regenerate models**

Run: `source env/bin/activate && make generate`
Expected: regenerates `ingestion/src/metadata/generated/schema/entity/services/ingestionPipelines/progressUpdate.py` (now with `globalCounters`, no `group*`) and the Java `ProgressUpdate`/`GlobalCounter` classes. No errors.

- [ ] **Step 3: Update the SSE test** — replace the SSE-shaped test (`tests/unit/workflow/test_progress_rendering.py` around :189-214, the one asserting `update.groupLabel == "Workspaces"`) with:

```python
    def test_progress_update_carries_global_counters(self):
        reg = ProgressRegistry()
        reg.set_total("Workspaces", 10)
        reg.track("Workspaces")
        reg.track("Workspaces")
        reg.track("Workspaces")
        counters = ProgressReporter(reg).global_counters()
        update = ProgressUpdate(
            runId="r1",
            timestamp=Timestamp(1),
            updateType=ProgressUpdateType.PROCESSING,
            globalCounters=[
                {"entityType": t, "done": d, "total": total} for t, d, total in counters
            ],
        )
        assert update.globalCounters[0].entityType == "Workspaces"
        assert update.globalCounters[0].done == 3
        assert update.globalCounters[0].total == 10
```

Ensure the test file imports `ProgressUpdate`, `ProgressUpdateType`, and `Timestamp` (it already imports them for the prior SSE test — keep those imports).

- [ ] **Step 4: Wire the mixin** — in `workflow_status_mixin.py`, replace lines 203-211 (`progress_data = ...` through the `ProgressUpdate(...)` close) with:

```python
                reporter = self._progress_reporter()
                progress_data = reporter.payload() if reporter is not None else None
                counters = reporter.global_counters() if reporter is not None else []

                progress_update = ProgressUpdate(
                    runId=self.run_id,
                    timestamp=Timestamp(int(datetime.now().timestamp() * 1000)),
                    updateType=update_type,
                    progress=progress_data if progress_data else None,
                    globalCounters=[
                        {"entityType": type_, "done": done, "total": total}
                        for type_, done, total in counters
                    ],
                )
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `source env/bin/activate && cd ingestion && python -m pytest tests/unit/workflow/test_progress_rendering.py -v`
Expected: PASS.

- [ ] **Step 6: Check for a UI consumer of the old fields**

Run: `cd /Users/apple/conductor/workspaces/OpenMetadata/manila && grep -rn "groupLabel\|groupTotal\|groupDone" openmetadata-ui/src/main/resources/ui/src --include=*.ts --include=*.tsx`
Expected: no matches. If any match exists, update it to read `globalCounters` (array of `{entityType, done, total}`) and note it in the commit; if none, proceed.

- [ ] **Step 7: Format and commit**

```bash
cd ingestion && make py_format
cd /Users/apple/conductor/workspaces/OpenMetadata/manila
git add openmetadata-spec/src/main/resources/json/schema/entity/services/ingestionPipelines/progressUpdate.json ingestion/src/metadata/generated ingestion/src/metadata/workflow/workflow_status_mixin.py ingestion/tests/unit/workflow/test_progress_rendering.py
git commit -m "feat: SSE ProgressUpdate.globalCounters array replaces single group axis"
```

---

### Task 4: Topology runner — track at close + reconcile on materialize

**Files:**
- Modify: `ingestion/src/metadata/ingestion/api/topology_runner.py` (`_process_node` :290-318; `_multithread_process_node` open site :236-239; `_multithread_process_entity` close site :388-394)
- Test: `ingestion/tests/unit/topology/test_topology_runner_progress.py`

**Interfaces:**
- Consumes: `progress.track(type_)`, `progress.is_reconcilable(type_)`, `progress.reconcile_scope_total(type_, scope, n)` (Task 1); existing `current_progress_path`, `_scope_path_for_node`, `_should_track_progress`.
- Produces: closing a container node bumps its type's global `done`; a reconcilable container node's real child count reconciles its parent-scoped global total.

**Critical context (the `databaseSchema` node is multithreaded):** the DB topology's `databaseSchema` node has `threads=True`, so in real ingestion it runs through `_multithread_process_node` (which already `list()`-materializes the producer at :224 and calls `open(parent_path, type, len)` at :239), while single-thread configs/tests run it through `_process_node`. Both paths therefore need the reconcile call. Eager-draining the schema producer is already what the multithread path does in production, so it is proven safe — unlike the predecessor's PowerBI leaf case, the schema producer has no per-yield teardown. `track` at close must likewise be in both the `_process_node` close site and the `_multithread_process_entity` close site.

- [ ] **Step 1: Write the failing tests** — add to `tests/unit/topology/test_topology_runner_progress.py` (reuse the file's existing harness for building a runner with a topology; mirror its existing style):

```python
    def test_closing_container_tracks_global_done(self):
        runner = self._make_runner_with_db_schema_table_topology()
        runner.progress.set_total("DatabaseSchema", 3)
        runner.progress.set_total("Database", 2)
        list(runner._iter())
        counters = dict((t, (d, total)) for t, d, total in runner.progress.global_counters())
        assert counters["DatabaseSchema"][0] >= 1
        assert counters["Database"][0] >= 1

    def test_reconcilable_container_reconciles_total(self):
        runner = self._make_runner_with_db_schema_table_topology()
        runner.progress.seed_scope_total("DatabaseSchema", "db1", 1)
        list(runner._iter())
        _, _, total = next(c for c in runner.progress.global_counters() if c[0] == "DatabaseSchema")
        assert total == runner._observed_schema_count_for("db1")
```

Note for the implementer: the existing test module already constructs a fake source with a Database→DatabaseSchema→Table topology and a `ProgressRegistry`. Build `_make_runner_with_db_schema_table_topology` and `_observed_schema_count_for` from that existing harness (the module sets `progress_tracking_enabled = True` and stubs the producers). If the existing harness exposes a single-database/two-schema fixture, assert the concrete numbers (e.g. `total == 2`) instead of the helper.

- [ ] **Step 2: Run tests to verify they fail**

Run: `source env/bin/activate && cd ingestion && python -m pytest tests/unit/topology/test_topology_runner_progress.py -k "tracks_global_done or reconciles_total" -v`
Expected: FAIL — global counters stay at their declared totals with `done == 0` (no `track`/reconcile wired yet).

- [ ] **Step 3: Add reconcile + track in `_process_node`** — replace the body of `_process_node` from line 296 (`if is_leaf and track_progress:`) through line 318 with:

```python
        reconcilable = (
            track_progress and not is_leaf and self.progress.is_reconcilable(entity_type_name)
        )
        if track_progress and (is_leaf or reconcilable):
            node_entities = list(self._run_node_producer(node) or [])
            self.progress.open(parent_path, entity_type_name, len(node_entities))
            if reconcilable and parent_path:
                self.progress.reconcile_scope_total(entity_type_name, parent_path[-1], len(node_entities))
        else:
            node_entities = self._run_node_producer(node) or []
            if track_progress:
                self.progress.open(parent_path, entity_type_name, None)

        for node_entity in node_entities:
            for stage in node.stages:
                yield from self._process_stage(stage=stage, node_entity=node_entity)

            for stage in node.stages:
                if stage.clear_context:
                    self.context.get().clear_stage(stage=stage)

            if track_progress and is_leaf:
                self.progress.advance(parent_path, entity_type_name)

            scope_path = self._scope_path_for_node(node, parent_path) if track_progress and not is_leaf else None
            yield from self.process_nodes(child_nodes)
            if scope_path is not None:
                self.progress.close(scope_path)
                self.progress.track(entity_type_name)
```

- [ ] **Step 4: Add track at the multithread close site** — in `_multithread_process_entity`, the block at lines 393-394:

```python
            if scope_path is not None:
                self.progress.close(scope_path)
```

becomes:

```python
            if scope_path is not None:
                self.progress.close(scope_path)
                self.progress.track(entity_type_name)
```

(`entity_type_name` is already a parameter of `_multithread_process_entity` — see its signature at :365.)

- [ ] **Step 4b: Add reconcile at the multithread open site** — in `_multithread_process_node`, the block at lines 238-239:

```python
        if track_progress:
            self.progress.open(parent_path, entity_type_name, node_entities_length)
```

becomes:

```python
        if track_progress:
            self.progress.open(parent_path, entity_type_name, node_entities_length)
            if self.progress.is_reconcilable(entity_type_name) and parent_path:
                self.progress.reconcile_scope_total(
                    entity_type_name, parent_path[-1], node_entities_length
                )
```

This covers the production path where `databaseSchema` (threads=True) is processed multithreaded; the `_process_node` reconcile in Step 3 covers single-thread configs and the unit tests.

- [ ] **Step 5: Run tests to verify they pass**

Run: `source env/bin/activate && cd ingestion && python -m pytest tests/unit/topology/test_topology_runner_progress.py -v`
Expected: PASS (new tests plus the file's existing progress tests — the lazy-iteration tests still hold because non-reconcilable containers keep `open(..., None)`).

- [ ] **Step 6: Format and commit**

```bash
cd ingestion && make py_format
git add src/metadata/ingestion/api/topology_runner.py tests/unit/topology/test_topology_runner_progress.py
git commit -m "feat(ingestion): runner tracks global done at scope close and reconciles reconcilable container totals"
```

---

### Task 5: Snowflake upfront declaration

**Files:**
- Modify: `ingestion/src/metadata/ingestion/source/database/snowflake/queries.py` (re-add `SNOWFLAKE_GET_SCHEMATA`)
- Modify: `ingestion/src/metadata/ingestion/source/database/snowflake/metadata.py` (imports :77-95, :117-118, :5; add helpers + `_declare_progress_totals`; call from `get_database_names`)
- Test: `ingestion/tests/unit/source/database/test_snowflake_progress_count.py`

**Interfaces:**
- Consumes: `progress.set_total`, `progress.seed_scope_total`, `progress.set_reconcilable` (Task 1); existing `self._filtered_database_names()`, `self.connection`, `self.source_config.useFqnForFiltering`, `filter_by_schema`, `fqn.build`.
- Produces: on first `get_database_names()`, the registry holds `Database` total = filtered DB count and `DatabaseSchema` total = sum of filtered schema counts (when the account SHOW succeeds), else a reconcilable `DatabaseSchema` counter.

- [ ] **Step 1: Re-add the query** — in `snowflake/queries.py`, before `SNOWFLAKE_GET_SCHEMA_COLUMNS` (the spot `8928ed4ab1` removed it), add:

```python
# Account-wide schema listing in a single round-trip (one connection, no
# per-database reconnect). Returns one row per schema with a `database_name`
# column. NOTE: SHOW is capped at 10k rows; accounts above that fall back to
# per-database enumeration via the caller's error handling.
SNOWFLAKE_GET_SCHEMATA = "SHOW TERSE SCHEMAS IN ACCOUNT"
```

- [ ] **Step 2: Write the failing tests** — replace `tests/unit/source/database/test_snowflake_progress_count.py`'s `test_get_database_names_pushes_no_container_totals` (the one asserting `progress.snapshot() is None` and `not hasattr(...)`) with:

```python
def test_declare_progress_totals_seeds_database_and_schema(snowflake_source):
    snowflake_source._filtered_database_names = lambda: ["db1", "db2"]
    snowflake_source._schema_names_by_database = lambda: {"db1": ["s1", "s2"], "db2": ["s3"]}
    snowflake_source._is_schema_filtered = lambda db, sch: False
    snowflake_source._declare_progress_totals()
    counters = dict((t, (d, total)) for t, d, total in snowflake_source.progress.global_counters())
    assert counters["Database"] == (0, 2)
    assert counters["DatabaseSchema"] == (0, 3)


def test_declare_progress_totals_applies_schema_filter(snowflake_source):
    snowflake_source._filtered_database_names = lambda: ["db1"]
    snowflake_source._schema_names_by_database = lambda: {"db1": ["keep", "drop"]}
    snowflake_source._is_schema_filtered = lambda db, sch: sch == "drop"
    snowflake_source._declare_progress_totals()
    counters = dict((t, (d, total)) for t, d, total in snowflake_source.progress.global_counters())
    assert counters["DatabaseSchema"] == (0, 1)


def test_declare_progress_totals_falls_back_when_account_show_unavailable(snowflake_source):
    snowflake_source._filtered_database_names = lambda: ["db1"]
    snowflake_source._schema_names_by_database = lambda: None
    snowflake_source._declare_progress_totals()
    assert snowflake_source.progress.is_reconcilable("DatabaseSchema") is True
    counters = dict((t, (d, total)) for t, d, total in snowflake_source.progress.global_counters())
    assert counters["Database"] == (0, 1)
    assert counters["DatabaseSchema"] == (0, None)
```

The `snowflake_source` fixture already exists in this file; it builds a `SnowflakeSource` stub with a `ProgressRegistry` (the file sets `source.__dict__["_progress_registry"] = ProgressRegistry()`). Keep that fixture.

- [ ] **Step 3: Run tests to verify they fail**

Run: `source env/bin/activate && cd ingestion && python -m pytest tests/unit/source/database/test_snowflake_progress_count.py -v`
Expected: FAIL — `AttributeError: ... has no attribute '_declare_progress_totals'`.

- [ ] **Step 4: Re-add imports** — in `snowflake/metadata.py`:
  - Line 5 typing import: change `from typing import Iterable, List, Optional, Tuple, cast` to `from typing import Dict, Iterable, List, Optional, Tuple, cast  # noqa: UP035`.
  - In the `snowflake.queries` import block (:77-94), add `SNOWFLAKE_GET_SCHEMATA,` alphabetically among the other `SNOWFLAKE_GET_*` names.
  - Line 118: change `from metadata.utils.filters import filter_by_database` to `from metadata.utils.filters import filter_by_database, filter_by_schema`.

- [ ] **Step 5: Add the module-level `_show_column` helper** — in `snowflake/metadata.py`, before the `class SnowflakeSource` declaration (where `8928ed4ab1` removed it), add:

```python
def _show_column(row, name: str):
    """Read a column from a Snowflake ``SHOW`` result row by name,
    case-insensitively (SHOW exposes lowercase column names)."""
    mapping = getattr(row, "_mapping", None)
    if mapping is not None:
        lowered = {str(key).lower(): value for key, value in mapping.items()}
        result = lowered.get(name.lower())
    else:
        result = getattr(row, name, None)
    return result
```

- [ ] **Step 6: Add the Snowflake helper methods** — inside `class SnowflakeSource`, near `_filtered_database_names` (after the method ending at line ~427, before `get_database_names`), add:

```python
    def _schema_names_by_database(self) -> "Optional[Dict[str, List[str]]]":  # noqa: UP006,UP045
        """``{database: [schema_names]}`` for every filtered database, from a
        single account-wide ``SHOW SCHEMAS`` — one round-trip, no per-database
        reconnect. Returns ``None`` when the account-level SHOW is unavailable
        (e.g. role privileges) so the caller can fall back to reconcile-only."""
        by_database: Dict[str, List[str]] = {db: [] for db in self._filtered_database_names()}  # noqa: UP006
        try:
            rows = self.connection.execute(text(SNOWFLAKE_GET_SCHEMATA)).fetchall()
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning(
                "SHOW SCHEMAS IN ACCOUNT failed (%s); progress schema total will reconcile during the walk.",
                exc,
            )
            return None
        for row in rows:
            database_name = _show_column(row, "database_name")
            schema_name = _show_column(row, "name")
            if database_name in by_database and schema_name is not None:
                by_database[database_name].append(str(schema_name))
        return by_database

    def _is_schema_filtered(self, database_name: str, schema_name: str) -> bool:
        """Whether a schema fails the schema filter pattern, matched the same way
        as the lazy producer (FQN or bare name per ``useFqnForFiltering``).
        Context-free: the FQN is built with the explicit database name."""
        schema_fqn = fqn.build(
            self.metadata,
            entity_type=DatabaseSchema,
            service_name=self.context.get().database_service,
            database_name=database_name,
            schema_name=schema_name,
        )
        filter_name = schema_fqn if self.source_config.useFqnForFiltering else schema_name
        return filter_by_schema(self.source_config.schemaFilterPattern, filter_name)

    def _declare_progress_totals(self) -> None:
        """Seed the run-level ``Database`` and ``DatabaseSchema`` global counters
        upfront. ``Database`` is the filtered DB count; ``DatabaseSchema`` is the
        post-filter schema count per database from the account-wide SHOW. When
        that SHOW is unavailable, the schema counter is marked reconcilable so the
        walk fills its total instead."""
        database_names = self._filtered_database_names()
        self.progress.set_total(Database.__name__, len(database_names))
        schemas_by_database = self._schema_names_by_database()
        if schemas_by_database is None:
            self.progress.set_reconcilable(DatabaseSchema.__name__)
        else:
            for database_name in database_names:
                kept = [
                    schema_name
                    for schema_name in schemas_by_database.get(database_name, [])
                    if not self._is_schema_filtered(database_name, schema_name)
                ]
                self.progress.seed_scope_total(DatabaseSchema.__name__, database_name, len(kept))
```

- [ ] **Step 7: Call it once from `get_database_names`** — at the top of `get_database_names` (line 437, before `for database_name in self._filtered_database_names():`), add:

```python
        if self.progress_tracking_enabled:
            self._declare_progress_totals()
```

- [ ] **Step 8: Run tests to verify they pass**

Run: `source env/bin/activate && cd ingestion && python -m pytest tests/unit/source/database/test_snowflake_progress_count.py -v`
Expected: PASS.

- [ ] **Step 9: Format and commit**

```bash
cd ingestion && make py_format
git add src/metadata/ingestion/source/database/snowflake/metadata.py src/metadata/ingestion/source/database/snowflake/queries.py tests/unit/source/database/test_snowflake_progress_count.py
git commit -m "feat(snowflake): declare Database/DatabaseSchema global totals upfront from account-wide SHOW SCHEMAS"
```

---

### Task 6: PowerBI migration to the keyed API

**Files:**
- Modify: `ingestion/src/metadata/ingestion/source/dashboard/dashboard_service.py` (`_declare_progress_groups` :218-220; `_close_group_progress` :232-235)
- Test: existing PowerBI/dashboard progress test (locate via grep in Step 1)

**Interfaces:**
- Consumes: `progress.set_total`, `progress.track`, `progress.close` (Task 1).
- Produces: PowerBI's `Workspaces` axis declared via `set_total`; each finished workspace calls `track("Workspaces")`. The `_open_group_progress`/`_advance_group_progress` tree helpers (per-workspace asset lines) are unchanged, so the 5 call-sites in `powerbi/metadata.py` need no edits.

- [ ] **Step 1: Locate the test that exercises the group helpers**

Run: `cd ingestion && grep -rln "_declare_progress_groups\|_close_group_progress\|set_group\|complete_group\|Workspaces" tests/unit`
Expected: identifies the PowerBI/dashboard progress test(s). Open the matched file(s).

- [ ] **Step 2: Update the test assertions** — wherever a test asserts the old single-group state (`registry.group_progress() == ("Workspaces", n, total)`), change it to the keyed form:

```python
    assert source.progress.global_counters() == [("Workspaces", n, total)]
```

(substitute the test's actual `n`/`total`). If a test calls `registry.set_group`/`complete_group` directly to set up state, change those to `set_total("Workspaces", total)` / `track("Workspaces")`.

- [ ] **Step 3: Run the test to verify it fails**

Run: `source env/bin/activate && cd ingestion && python -m pytest <matched_test_file> -v`
Expected: FAIL — `AttributeError: ... has no attribute 'set_group'`.

- [ ] **Step 4: Migrate the helpers** — in `dashboard_service.py`, replace `_declare_progress_groups` (:218-220):

```python
    def _declare_progress_groups(self, label: str, total: Optional[int]) -> None:  # noqa: UP045
        """Declare the grouping axis (e.g. workspaces) as a global counter and
        remember its label so completion can target it at scope close."""
        self.__dict__["_progress_counter_label"] = label
        self.progress.set_total(label, total)
```

and replace `_close_group_progress` (:232-235):

```python
    def _close_group_progress(self, group: str) -> None:
        """Count the finished group on its global counter and prune its subtree."""
        label = self.__dict__.get("_progress_counter_label")
        if label is not None:
            self.progress.track(label)
        self.progress.close([group])
```

Leave `_open_group_progress` and `_advance_group_progress` unchanged.

- [ ] **Step 5: Run the test to verify it passes**

Run: `source env/bin/activate && cd ingestion && python -m pytest <matched_test_file> -v`
Expected: PASS.

- [ ] **Step 6: Verify no stale references remain**

Run: `cd ingestion && grep -rn "set_group\|complete_group\|group_progress\|reporter.group\b" src/metadata tests/unit`
Expected: no matches.

- [ ] **Step 7: Format and commit**

```bash
cd ingestion && make py_format
git add src/metadata/ingestion/source/dashboard/dashboard_service.py tests/unit
git commit -m "feat(powerbi): migrate Workspace progress axis to keyed global counters"
```

---

### Task 7: Full-suite verification

**Files:** none (verification only).

- [ ] **Step 1: Run every affected suite together**

Run:
```bash
source env/bin/activate && cd ingestion && python -m pytest \
  tests/unit/test_progress_registry.py \
  tests/unit/workflow/test_progress_rendering.py \
  tests/unit/topology/test_topology_runner_progress.py \
  tests/unit/source/database/test_snowflake_progress_count.py -v
```
Expected: all PASS.

- [ ] **Step 2: Confirm no leftover old API anywhere**

Run: `cd /Users/apple/conductor/workspaces/OpenMetadata/manila && grep -rn "set_group\|complete_group\|group_progress\|groupLabel\|groupTotal\|groupDone" ingestion/src openmetadata-spec/src openmetadata-ui/src/main/resources/ui/src 2>/dev/null`
Expected: no matches (generated `progressUpdate.py` no longer has `group*`; if the UI grep flagged a consumer in Task 3 it was already migrated).

- [ ] **Step 3: Lint/format gate**

Run: `source env/bin/activate && cd ingestion && make py_format_check`
Expected: clean (matches CI).

---

## Self-Review

**Spec coverage:**
- Global counters keyed map → Task 1. ✓
- Totals upfront (C) for Snowflake → Task 5 (`seed_scope_total` from account SHOW). ✓
- Self-healing reconcile (B) → Task 1 (`reconcile_scope_total`, clamp) + Task 4 (runner reconcile on materialize). ✓
- `track` no-op for undeclared types → Task 1 test `test_track_unknown_type_is_noop`. ✓
- Framework increments done at close → Task 4 (both single + multithread paths). ✓
- Gated eager materialization preserves lazy default → Task 4 (`is_reconcilable` gate; non-reconcilable containers keep `open(..., None)`). ✓
- Scope-key convention (parent path / db) → Task 4 Step 3 (`parent_path[-1]`) matches Task 5 `seed_scope_total(..., database_name, ...)`. ✓
- PowerBI migration, call-sites unchanged → Task 6. ✓
- SSE `globalCounters` array + `make generate` + UI consumer check → Task 3. ✓
- Reinstate `SHOW TERSE SCHEMAS IN ACCOUNT` + filter helper → Task 5. ✓
- Tests across registry/runner/snowflake/powerbi → Tasks 1,2,4,5,6 + suite in Task 7. ✓

**Placeholder scan:** No TBD/TODO; every code step shows code; the two cross-referenced existing harnesses (topology runner test in Task 4, PowerBI test in Task 6) are located by grep and adapted in-place because their exact fixture names live in the test files — the steps give the concrete assertions to write.

**Type consistency:** `set_total(type_, total)`, `seed_scope_total(type_, scope, n)`, `reconcile_scope_total(type_, scope, observed)`, `track(type_)`, `is_reconcilable(type_)`, `global_counters() -> [(type, done, total)]` are used identically in Tasks 2/4/5/6. Snowflake uses `Database.__name__` / `DatabaseSchema.__name__` (= `"Database"`/`"DatabaseSchema"`), the same strings the runner derives via `_get_entity_type_for_node`, so framework `track`/reconcile and connector `seed` address the same counters. SSE item shape `{entityType, done, total}` matches the JSON schema in Task 3 and the mixin mapping in Task 3 Step 4.
</content>
</invoke>
