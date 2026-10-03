# What this package carries for apache/ossie

The converters do the conversion; this package does the transport. Where that line
has been crossed, it is recorded here — what we do instead of upstream, why, and what
would let the code be deleted.

Each entry is marked `# UPSTREAM:` in the source, so `grep -rn UPSTREAM: src/` finds
them all. Nothing here should become permanent architecture by default.

## Worked around, because the output is otherwise unusable

### 1. TMSL that Fabric refuses to import
**`fabric.py` · `to_tmsl`** — sets `defaultPowerBIDataSourceVersion: powerBI_V3`.

`convert_ossie_to_semantic_model` omits it, and Fabric rejects the whole import:
`Dataset_Import_FailedToImportDataset — Import from JSON supported for V3 models only`.
Upstream knows: `ossie_microsoft.engine.build_deployable` sets exactly this before
publishing, so the knowledge exists in the package but not in the converter.

*Delete when* the converter emits it. **This is the clearest bug of the three** — the
converter produces a document its own platform will not accept.

### 2. Snowflake rejects the metrics the converter emits
**`snowflake.py` · `nest_metrics_per_table`** — ~60 lines, including inferring which
table each metric belongs to.

`convert_ossie_to_snowflake` emits `metrics:` as a sibling of `tables:`. Snowflake's
native schema refuses it: `Unsupported expression in the definition of derived metric`.
Every metric must be nested under one table.

Upstream does not attribute metrics to tables at all. The inference here reads the
columns each metric's expression references and falls back to the fact table — it is a
working implementation that could be donated rather than kept.

*Delete when* the converter nests metrics itself. Recorded independently in the
`ossie_example` repo's NOTES.md, which hardcoded `METRICS_TABLE = "orders"` instead.

### 3. A downloaded Fabric model has no name
**`fabric.py` · `download`** — puts the item's name back before converting.

Fabric stores a model's name on the item, not in the TMSL: a definition read back has
only `compatibilityLevel` and `model`. `convert_semantic_model_to_ossie` then falls
back to a generic name, so every downloaded model is called `semantic_model` and
re-uploading one creates a second item under that name.

`convert_metric_view_to_ossie` already takes `model_name` for exactly this reason. The
Microsoft converter does not — pure asymmetry.

*Delete when* `convert_semantic_model_to_ossie` accepts a name.

## Ours by choice, and worth revisiting

An import-mode deployment used to live here, generating T-SQL from a field's Ossie
expression so that a calculated column could reach Fabric. It was removed: the only
model needing it had a field whose per-dialect expressions disagreed with each other
(`UPPER` / `INITCAP` / `LOWER`), which demonstrates dialect support rather than
modelling anything. A genuinely computed field belongs in a view over the source
table.

### 4. Rewriting the model to satisfy Unity Catalog
**`_model.py` · `drop_duplicate_join_keys`**

Fabric needs a join key present on both datasets or it drops the relationship; a Metric
View refuses the duplicate dimension. We edit the user's model so one file serves both,
and warn.

It is policy, in a library that otherwise only moves files. It is also the only place
we change the meaning of what someone wrote. Defensible — the alternative is two copies
of every model, which `ossie_example` demonstrates the cost of — but it should stay
visible and opt-out (`dedupe_join_keys=False`).

### 5. Qualifying bare source names
**`_model.py` · `qualify_sources`**

Platforms disagree on how many parts a table name has — two for Fabric, three for Unity
Catalog and Snowflake — so a fully qualified `source:` cannot be portable. Bare names
get the connection's prefix.

Closest to legitimate of the three: it is deployment targeting, like a connection
string. Noted because it still edits the model.

## Seen but not worked around

Reproduced while building this; no code here depends on them.

- **`ossie-databricks` drops silently.** `datatype`, `label`, field and metric
  `ai_context`, and every non-`DATABRICKS` dialect vanish with no warning, while
  `dimension.is_time` and dataset descriptions *are* reported. The reporting machinery
  exists and is not used on those paths.
- **`ossie-dbt` has four bugs**, all still present at the pinned commit and all
  reported as zero issues by the converter: `order_count` attaches to the wrong
  semantic model, `COUNT(*)` becomes `expr: '*'` which compiles to invalid SQL,
  `agg_time_dimension` is never set, and no time spine is configured. Documented with
  reproductions in the `ossie_example` repo.
- **`ossie-microsoft` drops `label`** with "a Power BI semantic model has nowhere to
  record it". TMSL has culture translations for display names, so arguably it does.
- **`ossie-snowflake` has no reverse converter**, which is why `Snowflake` is
  upload-only.
- **The `apache-ossie-*` names are unregistered on PyPI**, so anyone could claim them.
  A supply-chain exposure for an incubating project, not a bug.
