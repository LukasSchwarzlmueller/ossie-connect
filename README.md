# ossie-connect

Deploy [Apache Ossie](https://ossie.apache.org/) semantic models to Microsoft Fabric,
Databricks and Snowflake — and turn semantic models that already exist in Fabric or
Databricks into Ossie files.

The [apache-ossie converters](https://github.com/apache/ossie/tree/main/converters) already
translate between Ossie and each platform's own format, but they are deliberately offline -
`ossie-microsoft` describes itself as "pure offline transforms; no Power BI connection
needed". This package adds the part they leave out: authenticating, creating or updating the
object in a live workspace, and fetching it back.

```bash
pip install ossie-connect                # Fabric and Databricks, both included
pip install "ossie-connect[snowflake]"   # the above, plus Snowflake
```

Only Snowflake is an extra, because its connector roughly doubles the install - 18
packages and 36 MB becomes 36 packages and 80 MB. Fabric talks to the REST API over
`urllib` and needs nothing, and `databricks-sdk` is small, so neither is worth making
optional.

## Python

Define a connection once, then move files across it.

```python
from ossie_connect import Fabric, Databricks

fabric = Fabric(workspace="8f1c…", lakehouse="2b40…")
fabric.upload("model.yaml")                      # -> Fabric item id
fabric.download("sales_demo", "model.yaml")      # -> Ossie YAML

dbx = Databricks(catalog="main", schema="sales", warehouse_id="a1b2…")
dbx.upload("model.yaml")                         # -> main.sales.sales_demo
dbx.download("sales_demo", "model.yaml")

snow = Snowflake(database="OSSIE_DEMO", schema="PUBLIC")
snow.upload("model.yaml")                        # -> OSSIE_DEMO.PUBLIC.sales_demo
```

**Snowflake is upload-only.** `apache-ossie-snowflake` converts Ossie to Snowflake's
Semantic View format but ships nothing going the other way, so `Snowflake` has no
`download` at all rather than one that raises. The protocols reflect that: `Fabric` and
`Databricks` are `Connection`, `Snowflake` is only `SupportsUpload`.

`upload` takes a path or the YAML itself. `download` returns the YAML and writes it only if
you pass a second argument. Both are idempotent: uploading twice updates the existing object
rather than creating a second one.

### Configuration is optional

`from_env()` is a convenience, not the way in. Every connection takes its settings
directly, so a script can hold them itself, read them from your own config, or compute
them:

```python
Databricks(catalog="main", schema="sales", warehouse_id="a1b2…")
Fabric(workspace="8f1c…", lakehouse="2b40…")
Snowflake(database="OSSIE_DEMO", schema="PUBLIC",
          account="…", user="…", password="…")
```

`from_env()` reads the variables in the table below, and takes overrides:
`Databricks.from_env(schema="marketing")`. `load_env()` loads a `.env` file first if you
want one. None of it is required.

### Deploying to whatever is configured

`configured_connections()` returns the platforms this environment has settings for,
leaving out the ones it does not. Each connection carries its own `platform` name:

```python
load_env()
for connection in configured_connections():
    print(connection.platform, connection.upload("model.yaml"))
```

The same script then works on a machine set up for one platform and a machine set up
for three. It reports what is *configured*, not what will authenticate - checking that
would mean acquiring a token from each - so an upload can still fail. It takes the same
overrides as `from_env()`, so `configured_connections(schema="staging")` re-points
everything it found.

### Several targets in one script

`at()` re-points a connection, sharing the authenticated client rather than building a
second one:

```python
dbx = Databricks.from_env()

dbx.upload(models["sales_demo"])                       # main.sales
dbx.at(schema="marketing").upload(models["campaigns"]) # main.marketing
dbx.at(catalog="prod", schema="sales").upload(models["sales_demo"])

for schema in ("dev", "staging", "prod"):              # one login, three deploys
    dbx.at(schema=schema).upload(models["sales_demo"])
```

The original is never mutated - `at()` returns a new connection. It exists on all three
platforms: `Fabric.at(workspace=…, lakehouse=…)` shares the token, so several targets
cost one `az` call, and `Snowflake.at(database=…, schema=…)` shares the open session.

### Does it need to run in parallel?

Measured against real accounts, one upload costs roughly a second once connected -
Databricks 1.2s, Snowflake 0.8s. A handful of models across three platforms is a few
seconds, so sequential is fine and simpler.

If you reach a few dozen models and want concurrency, write it yourself - uploads are
network-bound, so threads are enough, and the pieces you need are already safe:

```python
from concurrent.futures import ThreadPoolExecutor

dbx = Databricks.from_env()
with ThreadPoolExecutor(max_workers=8) as pool:
    pool.map(lambda m: dbx.upload(m), models.values())
```

Lazy credential setup is locked, so connections sharing a client through `at()` acquire
one token and build one client however many threads use them. Uploads are idempotent,
so a retry after a partial failure is safe. Fabric requests retry themselves on 429 and
5xx, honouring `Retry-After` - throttling is a wait, not a failure.

### A folder of models

`Models` maps a directory of Ossie files by the name *inside* each file, which is not
necessarily the filename:

```python
from ossie_connect import Models

models = Models("models/")
list(models)                            # ['finance_demo', 'marketing_demo', 'sales_demo']
fabric.upload(models["sales_demo"])     # values are paths, which upload takes

for name, path in models.items():       # deploy a subset
    if name.startswith("sales_"):
        fabric.upload(path)
```

It is an ordinary `Mapping`, so `in`, `len()`, `.keys()` and `.items()` all work. Files
that are not Ossie models are ignored; two files declaring the same model name is an
error rather than a silent last-one-wins.

Warnings from a conversion - a dropped field, a placeholder used - are for whoever runs
the script, but Python prints them with a file path and a source echo. One call fixes
the format for the whole program:

```python
from ossie_connect import plain_warnings

plain_warnings()
# warning: Databricks ossi.test: dropped join-key field(s) ... customers.customer_id
```

Warnings name the connection that raised them. They go to stderr while your own output
goes to stdout, so the two interleave unpredictably - especially through a pipe, where
stdout is buffered and stderr is not. Ordering cannot be relied on, so each line says
for itself which platform and target it came from. `connection.platform` and
`connection.target` are the same two values, if you want them in your own output.

It is opt-in rather than done on import, because warning formatting belongs to the
program, not to a library it happens to use. To attribute warnings to a particular step
instead, collect them with `warnings.catch_warnings(record=True)` and filter on
`warning.filename` - `ossie-models/example.py` does that.

Converting without uploading:

```python
fabric.to_tmsl("model.yaml")              # the model.bim that would be sent
dbx.create_statement("model.yaml")        # the CREATE VIEW that would be run
```

## Command line

```bash
ossie-connect check    fabric                  # settings right? writes nothing
ossie-connect list     fabric                  # what is deployed there
ossie-connect upload   fabric     model.yaml
ossie-connect delete   fabric     sales_demo
ossie-connect upload   fabric     models/              # every model in the folder
ossie-connect download fabric     sales_demo -o model.yaml
ossie-connect upload   databricks model.yaml --dry-run
ossie-connect download databricks sales_demo          # to stdout
```

`list` names what is deployed, and `delete` removes one. `check` verifies the settings
describe something real and writes nothing, exiting
non-zero on an error. `check` is the only command that contacts the platform without
creating anything - `--dry-run` proves the conversion but talks to nobody, so it cannot
catch a wrong lakehouse, an expired token or a missing schema. `connection.check()` is
the same thing from Python, returning findings rather than printing them.

Given a folder, `upload` sends every Ossie model in it, named by what is inside each
file. One failure does not stop the rest - uploads are idempotent, so seeing every
problem at once and re-running beats one failure per run. The exit code is non-zero if
any model failed. Selecting a *subset* is deliberately not a flag; that is what the
`Models` mapping above is for.

Settings come from the environment and a `.env` file in the working directory; every one has
a flag that overrides it. Add `--warnings` to see what a conversion could not carry across.

## Configuration

| Variable | Used by | Notes |
|---|---|---|
| `FABRIC_WORKSPACE_ID` | Fabric | workspace to work in |
| `FABRIC_LAKEHOUSE_ID` | Fabric | lakehouse item id; see Direct Lake below |
| `FABRIC_LAKEHOUSE_WORKSPACE_ID` | Fabric | only if the lakehouse is in another workspace |
| `FABRIC_SCHEMA` | Fabric | default `dbo` |
| `FABRIC_TOKEN` | Fabric | overrides the Azure CLI |
| `DATABRICKS_CATALOG` / `DATABRICKS_SCHEMA` | Databricks | where the Metric View is created |
| `DATABRICKS_WAREHOUSE_ID` | Databricks | SQL warehouse; needed to upload, not to download |
| `DATABRICKS_HOST` / `DATABRICKS_TOKEN` | Databricks | read by the Databricks SDK itself |
| `SNOWFLAKE_DATABASE` / `SNOWFLAKE_SCHEMA` | Snowflake | where the Semantic View is created |
| `SNOWFLAKE_ACCOUNT` / `SNOWFLAKE_USER` / `SNOWFLAKE_PASSWORD` | Snowflake | account auth |
| `SNOWFLAKE_ROLE` | Snowflake | optional; falls back to your default role |
| `SNOWFLAKE_WAREHOUSE` | Snowflake | optional; default `OSSIE_COMPUTE_WH`, created if missing |

Fabric authenticates with the Azure CLI by default - run `az login` and no secret needs
storing. Databricks uses the SDK's own resolution: environment variables or a profile in
`~/.databrickscfg`.

## What happens on each platform

|  | Fabric | Databricks | Snowflake |
|---|---|---|---|
| becomes | a semantic model item | a Metric View | a native Semantic View |
| upload | `POST /items`, then `updateDefinition` | `CREATE OR REPLACE VIEW … WITH METRICS` | `SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML` |
| download | `getDefinition?format=TMSL` | `view_definition`, else `SHOW CREATE TABLE` | **not possible** |
| round trip | lossy, see below | partial, see below | n/a |
| compute needed | no | to upload only | creates its own warehouse |

Fabric transfers use TMSL rather than TMDL, because TMSL is a single `model.bim` part: no
.NET assemblies and no `tom` extra are involved on either leg.

### Direct Lake or import

Fabric uploads are Direct Lake by default. It is the better shape - no copy of the
data, no credentials - but it cannot hold a **calculated column**, which is any field
whose expression is not a plain column reference. A model with one is refused before
anything is sent, naming the field.

`Fabric(mode="import")` deploys such a model, rewriting the partitions to read the
lakehouse's SQL endpoint instead:

```python
Fabric.from_env(mode="import").upload("model.yaml")   # or FABRIC_MODE=import
```

The trade is credentials. The model deploys, but **refreshing it fails** until a cloud
connection is bound to that SQL endpoint, which Fabric will not infer:

> We cannot refresh this semantic model because this semantic model uses a default data
> connection without explicit connection credentials.

That is a one-off step in the model's settings in Fabric, or through the Power BI
connections API. `ossie-connect` warns about it on every import-mode upload rather than
doing it, since it means handling someone else's credentials.

### Direct Lake, and why the lakehouse id matters

Fabric uploads produce Direct Lake partitions. The lakehouse is identified by the OneLake URL
built from `lakehouse=` (its *item id*), not by any name. Leave it out and the converter
writes a zero-GUID placeholder: the model uploads, but it cannot refresh. The lakehouse must
already contain the tables the model names, under `schema`.

### Source qualification

Platforms disagree on how many parts a table name has - Fabric keeps schema and table, Unity
Catalog wants catalog, schema and table. A model that names its tables bare is qualified with
the connection's own prefix on the way out, so one file works against both:

```yaml
datasets:
  - name: orders
    source: orders        # -> dbo.orders on Fabric, main.sales.orders on Databricks
```

A model that already qualifies its sources is left alone.

## One file, both platforms

Fabric and Unity Catalog make opposite demands of the same join column, and a model
written for one fails on the other:

- A **Fabric relationship needs the key present as a field on both datasets.** Remove it
  and the relationship is dropped silently - the model uploads with no join at all.
- A **Metric View flattens every dataset into one namespace and refuses duplicate
  dimension names**, so `orders.customer_id` and `customers.customer_id` collide and the
  conversion fails outright.

`ossie-connect` reconciles this on the Databricks path rather than making you keep two
copies of the model: on the `to` side of a relationship it drops the key field and keeps
it in `primary_key`, so the join still resolves and the fact-side column is untouched.
Every removal is reported as a warning. Pass `dedupe_join_keys=False` to see the
converter's own error instead.

Write the model the way Fabric needs it - key present on both sides - and both platforms
work from that one file.

### Other things to know before writing a model

- **Databricks and Snowflake pin the Ossie version.** Both converters accept only
  `version: 0.2.0.dev0`.
- **Snowflake needs a `datatype` on every field.** It is silently omitted otherwise and
  only surfaces as a validation failure when the view is created.
- **Snowflake requires metrics nested under one table.** The converter emits a
  top-level `metrics:` list, which Snowflake rejects with "Unsupported expression in the
  definition of derived metric". `ossie-connect` nests each metric under the table whose
  columns its expression references, falling back to the fact table for expressions like
  `COUNT(*)` that name no column. Pass `nest_metrics=False` to see the raw converter
  output.
- **Qualify SQL expressions on joined tables.** A `DATABRICKS`-dialect expression like
  `LOWER(customer_name)` on a joined dataset is passed through unrewritten and Databricks
  rejects it with `UNRESOLVED_COLUMN`. Write `LOWER(customers.customer_name)`. The
  converter warns for any non-trivial expression, qualified or not.

## What a round trip preserves

**Databricks round-trips what a Metric View can hold, and no more.** Metric View →
Ossie → Metric View is byte-for-byte lossless; anything a Metric View has that Ossie
lacks a field for is stashed in `custom_extensions[DATABRICKS]` and restored.

Ossie → Metric View → Ossie is a different question, because an Ossie model carries
more than a Metric View can express. Measured against a real workspace, what survives:

| | survives |
|---|---|
| metrics, their descriptions and synonyms | yes |
| field synonyms (`ai_context`) | yes |
| the `DATABRICKS` dialect expression | yes |
| relationships, as joins | yes |
| `datatype`, `label`, `dimension.is_time` | **no** |
| `ANSI_SQL`, `DAX`, `SNOWFLAKE` dialects | **no** - a Metric View holds one expression |
| dataset-level `description` | **no** - only one top-level comment exists |

Only `is_time`, the dataset descriptions and the join-key change are reported; the rest
goes quietly. `warn=True` shows what is reported.

Two structural changes are worth expecting. The fact dataset is renamed after the model,
since a Metric View's YAML carries no model name of its own - `source` is untouched, so
re-uploading still targets the right table. And joined columns come back on the fact
dataset rather than the one they were defined on, because a Metric View flattens every
dataset into one namespace.

**Reading without a warehouse is lossier still.** `tables.get().view_definition` returns
a normalized form with `synonyms` and `comment` stripped - 646 characters against 1149
for the same view. `download` therefore prefers `SHOW CREATE TABLE`, and warns when it
has no warehouse to run it on.

**Fabric is lossier**, and worth knowing before you rely on a round trip:

- **`label` on a field is dropped** - a semantic model has nowhere to record a display
  name distinct from the column name.
- **`datatype` on a metric is dropped** - Power BI infers a measure's type from its DAX.
- **Only one dialect survives per expression.** A field carrying `ANSI_SQL`, `SNOWFLAKE`,
  `DATABRICKS` and `DAX` comes back with whichever one Fabric stored.
- **SQL-only metrics are translated.** Fabric generates DAX for a metric that has none and
  keeps the original SQL in `OssieExpression` annotations, which return inside
  `custom_extensions` rather than as an `ANSI_SQL` dialect.
- **Relationships are renamed** to their column-derived form; the original name survives
  in `custom_extensions`.
- **Everything else unmapped lands in `custom_extensions`** rather than being dropped,
  which is why a second round trip changes much less than the first.

Pass `warn=True` (or `--warnings`) and every one of these is reported as it happens.

## Status

| | upload | download | verified against a real platform |
|---|---|---|---|
| Databricks | yes | yes | **yes** - deploys, and the Metric View returns the right numbers |
| Snowflake | yes | not possible | **yes** - deploys, and the Semantic View returns the same numbers |
| Fabric | yes | yes | **yes** - deploys, stores what was sent, and its DAX compiles and evaluates |

133 tests run against in-memory fakes, covering conversion, request shapes,
long-running operations, preflight and the CLI. A separate repository runs the live
tests: it seeds tables on Databricks and Snowflake and asserts the deployed semantic
layer computes the expected figures, and on Fabric it reads the deployed TMSL back and
checks the measures, relationships and partitions are the ones it sent.

On Fabric the model is also published, refreshed - which is what compiles the DAX - and
every measure evaluated, through upstream's `validate_with_engine`. That covers the
measures this package's converter generates from SQL, such as `AVERAGE('orders'
[order_amount])` for a metric with no DAX of its own.

Not yet verified: a Fabric model reading **real rows**. Evaluating measures uses inline
sample data, so the figures are not the demo figures; asserting those needs a lakehouse
holding the tables, which Databricks and Snowflake both cover instead.

## Dependencies, and a change coming

The converters are pinned to one upstream commit rather than tracked. Two things make a
bump non-trivial:

- **Upstream now maintains the Databricks converter in Java.** `converters/databricks`
  has split into `java/` and `python/`; the Java one is "the maintained implementation"
  and the Python one this package uses is "to be deprecated". The path change alone
  breaks the pin. Conversion is kept behind `_converters.DatabricksConverter`, so moving
  to the Java CLI means implementing its two methods and passing
  `Databricks(converter=...)` - the connection does not change.
- **`_fabric_api` reuses three private helpers of `ossie_microsoft.engine`** for the
  HTTP layer, documented at the top of that module. A bump could rename them; the tests
  exercise all three.

## Licence

Apache 2.0.
