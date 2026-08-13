# nifi-iceberg-read-bundle — native Iceberg read processors (`GetIceberg` + `QueryIceberg`)

NiFi's stock Iceberg bundle is write-only — `PutIceberg` / `PutIcebergCDC`, no read side. This
bundle is that missing read side: two processors that plug into the **same `IcebergCatalogService`**
(REST or Hadoop) the stock bundle already uses, and emit rows through any configurable **Record
Writer**. Built for the CDP Data Share consumer case (read-capacity identity, vended credentials),
validated first against a local REST/Hadoop + MinIO rig, then live on a CDP Data Share.

- **`GetIceberg`** — reads a whole table (optional column projection); the straight read
  counterpart to `PutIceberg`.
- **`QueryIceberg`** — runs SQL `SELECT`s through Apache Calcite, pushing predicates and
  projections down into the Iceberg scan so filters prune files and manifests at the metadata layer.

## `GetIceberg` — read a table

Built by taking the raw `PutIceberg` source, renaming everything `GetIceberg`, ripping out the
put guts (Kerberos/UGI wrapping, RecordReader, task writers, commit retries) and putting in get
guts (`catalog.loadTable` → `IcebergGenerics.read(table)` → Iceberg-to-NiFi record conversion →
Record Writer).

Source processor (no input). Properties: `Catalog Service` (`IcebergCatalogService`),
`Catalog Namespace`, `Table Name`, `Record Writer` (`RecordSetWriterFactory`), optional `Columns`
projection. **Dynamic properties** are passed straight to the Iceberg catalog client — the escape
hatch for object-store specifics (`io-impl`, `s3.endpoint`, `s3.path-style-access`,
`client.region`, …) when the REST server doesn't vend them. Emits one FlowFile per trigger with
`record.count`, `iceberg.catalog.namespace`, `iceberg.table.name` attributes and a provenance
RECEIVE event on the table location. Two relationships — `success` for the rows and `failure` for
a diagnostic FlowFile — deliberately mirror the stock `PutIceberg` write processor, so a read side
and a write side present the same relationship surface on the canvas.

## `QueryIceberg` — SQL with predicate & projection pushdown

`QueryIceberg` runs SQL `SELECT`s against the table in the shape of NiFi's `QueryRecord`: **each
dynamic property is a query, routed to a same-named relationship.** Also a source processor
(no input); one FlowFile per query out through the Record Writer.

- **Fixed properties** — `Catalog Service`, `Catalog Namespace`, `Table Name` (the name the SQL
  references), `Record Writer`, `Default Precision` / `Default Scale` (for decimal literals), and
  `Include Zero Record FlowFiles`.
- **Dynamic properties, two kinds:**
  - Any name **not** prefixed `catalog.` is a SQL query; its results route to a relationship of
    that name (a `delayed` property → a `delayed` relationship). A `failure` relationship carries a
    diagnostic FlowFile (`iceberg.query.error`) for a bad query or table load.
  - A **`catalog.<property>`** name (e.g. `catalog.s3.endpoint`, `catalog.s3.path-style-access`,
    `catalog.client.region`) is stripped of its prefix and passed to the Iceberg catalog client —
    the same object-store escape hatch `GetIceberg` exposes, namespaced so it can't be mistaken for
    a query.
- **The engine is Apache Calcite** (1.40.0, matching NiFi 2.6.0). The table is handed to Calcite as
  a `ProjectableFilterableTable` (`IcebergTable`), so Calcite pushes down the projected columns and
  the `WHERE` filter; a translator turns as much of that filter as it can into a native Iceberg scan
  `Expression`. What can't be pushed (functions like `UPPER(...)`, non-prefix `LIKE`, negation on a
  nullable column) is left to Calcite as a **residual filter** — so results are always correct;
  pushdown only changes *how much data is read*, never *which rows come back*. v1 pushes
  `= <> < <= > >= IS [NOT] NULL IN`, prefix `LIKE`, and `AND` / `OR` / `NOT` over string, numeric,
  boolean and decimal columns.
- **Every FlowFile is annotated so you can watch the optimizer work:** `iceberg.pushdown.filter`
  (the expression that reached Iceberg; empty ⇒ the whole `WHERE` ran as a residual),
  `iceberg.pushdown.columns` (the projected scan), and the scan counters
  `iceberg.scan.result.data.files`, `iceberg.scan.skipped.data.files`,
  `iceberg.scan.skipped.data.manifests` (the pruning proof) — plus `record.count`, `mime.type`,
  `QueryIceberg.query` (the producing query/relationship), `iceberg.catalog.namespace` and
  `iceberg.table.name`.

## Shared components

- **`IcebergCatalogFactory`** — REST and HADOOP catalogs, used by both processors. Two deliberate
  divergences from the stock factory: the OAuth token is **null-guarded** (a disabled/failed
  provider fails with a clear message instead of an NPE inside Iceberg's `EnvironmentUtil`), and
  `header.X-Iceberg-Access-Delegation: vended-credentials` is always sent so a CDP datashare
  vends S3 read credentials on `loadTable`.
- **`IcebergToRecordConverter`** — Iceberg schema/rows → NiFi `RecordSchema`/`MapRecord`
  (the reverse direction of the stock bundle's `IcebergRecordConverter`).

## The classloader trick that makes it drop-in

The NAR declares the **CFM** iceberg services-api NAR as its parent:

```
Nar-Dependency-Group: org.apache.nifi
Nar-Dependency-Id: nifi-iceberg-services-api-nar
Nar-Dependency-Version: 2.6.0.4.3.4.0-234
```

so the *live* `RESTCatalogService` instance on a CFM NiFi satisfies the `catalog-service`
property directly. Everything else (Iceberg 1.7.2, parquet, hadoop-common, jackson) is bundled
*inside* this NAR — which sidesteps the CFM `iceberg-core 1.5.2` × `jackson-databind 2.20.1`
`PropertyNamingStrategy$KebabCaseStrategy` conflict entirely: no jackson-fix image needed for
this processor.

## Local dependency bootstrap (once per dev machine)

The two CFM artifacts aren't on any public Maven repo — extract them from the running CFM NiFi
pod (jars under `work/nar/extensions/...` are **symlinks** into `work/nar-lib/`, so stream the
real file, don't tar the directory):

```bash
POD=mynifi-0 NS=cfm-streaming V=2.6.0.4.3.4.0-234
kubectl exec $POD -n $NS -c nifi -- base64 \
  /opt/nifi/nifi-current/work/nar/extensions/nifi-iceberg-services-api-nar-$V.nar-unpacked/NAR-INF/bundled-dependencies/nifi-iceberg-services-api-$V.jar \
  | base64 -d > nifi-iceberg-services-api.jar

mvn install:install-file -Dfile=nifi-iceberg-services-api.jar \
  -DgroupId=org.apache.nifi -DartifactId=nifi-iceberg-services-api -Dversion=$V \
  -Dpackaging=jar -DgeneratePom=true
```

Then repackage the unpacked NAR dir (original `META-INF/MANIFEST.MF` + the real jar under
`NAR-INF/bundled-dependencies/`) with `jar cfm` and install it as `-Dpackaging=nar` with a pom
that declares packaging `nar`, the `nifi-nar-maven-plugin` extension, and dependencies on
`nifi-iceberg-services-api` + `nifi-record-serialization-service-api` + `nifi-oauth2-provider-api`
(the interfaces the real parent-NAR chain provides at runtime; the nar plugin's doc generator
needs them on its classpath).

## Build and deploy

```bash
mvn clean install -Denforcer.skip=true     # 46 TestRunner + unit tests; JaCoCo report under target/site/jacoco
# mvn -Denforcer.skip=true clean verify     # also runs the coverage gate: fails below 80% bundle line coverage
kubectl cp -c nifi nifi-iceberg-read-nar/target/nifi-iceberg-read-nar-1.0.3-SNAPSHOT.nar \
  cfm-streaming/mynifi-0:/opt/nifi/nifi-current/data/extensions/
```

The target dir is `nifi.nar.library.autoload.directory` (`./data/extensions` on this CFM build) —
the NAR hot-loads in ~10s, **no restart**. Note NiFi will not re-register a same-version
overwrite: bump the bundle version for every redeploy, then point the processor at the new
bundle version.

## test-rig/ — local validation without CDP credentials

- `iceberg-rest-rig.yaml` — `tabulario/iceberg-rest` + MinIO in an `iceberg-demo` namespace.
- `seed-airlines-job.yaml` — pyiceberg Job seeding `demo.airlines` with 3 rows (AA/DL/UA),
  mirroring the `poc_uc2.airlines` datashare table.
- `seed-flights-job.yaml` — pyiceberg Job seeding `demo.flights`, ~120k rows partitioned by a
  string `flight_month` (`'2026-01'`…`'2026-12'`) as 12 monthly appends → one data file per month.
  A `QueryIceberg` query with `WHERE flight_month = '2026-03'` then prunes 11 of 12 months at the
  manifest layer (`iceberg.scan.skipped.data.manifests = 11`, `iceberg.scan.result.data.files = 1`).
- `build-demo-pg.sh` — builds the `GetIcebergDemo` PG via the NiFi REST API:
  `RESTCatalogService` (no OAuth) + `JsonRecordSetWriter` + `GetIceberg` → funnel.

The tabulario fixture does **not** vend `io-impl`/S3 config through `/v1/config`, so the demo
sets them as dynamic properties — plain (`s3.endpoint`, …) on `GetIceberg`, `catalog.`-prefixed
(`catalog.s3.endpoint`, …) on `QueryIceberg`. Against a real CDP datashare REST catalog the vended
config + credentials make those unnecessary — that's the point of the vended-credentials header.

Validated against CFM `2.6.0.4.3.4.0-234`: one FlowFile, `record.count=3`, JSON array of the
3 airlines, provenance `RECEIVE s3://warehouse/demo/airlines`.

## Pointing it at a CDP Data Share

Same PG shape plus the Knox OAuth chain: a `StandardOauth2AccessTokenProvider`
(client-credentials against the Knox token endpoint) wired into `RESTCatalogService`'s
`OAuth2 Access Token Provider`, `Catalog URI` = the datashare `iceberg-rest` endpoint,
namespace/table = `poc_uc2`/`airlines`. No dynamic S3 properties needed — the datashare vends the
S3 read credentials in the `loadTable` response, unlocked by the
`X-Iceberg-Access-Delegation: vended-credentials` header the factory always sends.

Against a live CDP Data Share this reads end to end: `GetIceberg` on `poc_uc2.airlines` returns a
single FlowFile whose content is a JSON array of the three airline rows — the same three rows a
Spark or SSB client sees through that catalog, now through a native NiFi processor with no
`InvokeHTTP` glue.
