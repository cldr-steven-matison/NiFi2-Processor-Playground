# nifi-geticeberg-bundle — a native `GetIceberg` read processor

NiFi's stock Iceberg bundle is write-only: `PutIceberg` / `PutIcebergCDC`, no read processor.
`GetIceberg` is the read counterpart — it plugs into the **same `RESTCatalogService`** (or any
`IcebergCatalogService`) the stock bundle uses, scans a table with the Iceberg API, and emits the
rows through a configurable **Record Writer**. Built for the CDP Data Share consumer case
(read-capacity identity, vended credentials), validated first against a local REST catalog +
MinIO rig.

Built by taking the raw `PutIceberg` source, renaming everything `GetIceberg`, ripping out the
put guts (Kerberos/UGI wrapping, RecordReader, task writers, commit retries) and putting in get
guts (`catalog.loadTable` → `IcebergGenerics.read(table)` → Iceberg-to-NiFi record conversion →
Record Writer).

## How it works

- **`GetIceberg`** — source processor (no input). Properties: `Catalog Service`
  (`IcebergCatalogService`), `Catalog Namespace`, `Table Name`, `Record Writer`
  (`RecordSetWriterFactory`), optional `Columns` projection. **Dynamic properties** are passed
  straight to the Iceberg catalog client — the escape hatch for object-store specifics
  (`io-impl`, `s3.endpoint`, `s3.path-style-access`, `client.region`, …) when the REST server
  doesn't vend them. Emits one FlowFile per trigger with `record.count`,
  `iceberg.catalog.namespace`, `iceberg.table.name` attributes and a provenance RECEIVE event
  on the table location.
- **`IcebergCatalogFactory`** — REST and HADOOP catalogs. Two deliberate divergences from the
  stock factory: the OAuth token is **null-guarded** (a disabled/failed provider fails with a
  clear message instead of an NPE inside Iceberg's `EnvironmentUtil`), and
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
mvn clean install -Denforcer.skip=true     # includes the HadoopCatalog TestRunner IT (3 rows)
kubectl cp -c nifi nifi-geticeberg-nar/target/nifi-geticeberg-nar-1.0.1-SNAPSHOT.nar \
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
- `build-demo-pg.sh` — builds the `GetIcebergDemo` PG via the NiFi REST API:
  `RESTCatalogService` (no OAuth) + `JsonRecordSetWriter` + `GetIceberg` → funnel.

The tabulario fixture does **not** vend `io-impl`/S3 config through `/v1/config`, so the demo
sets them as GetIceberg dynamic properties. Against a real CDP datashare REST catalog the vended
config + credentials make those unnecessary — that's the point of the vended-credentials header.

Validated result (CFM `2.6.0.4.3.4.0-234`, 2026-08-12): one FlowFile, `record.count=3`, JSON
array of the 3 airlines, provenance `RECEIVE s3://warehouse/demo/airlines`.

## Pointing it at a CDP Data Share (the #154 target)

Same PG shape plus the Knox OAuth chain from the #152 recipe: `StandardOauth2AccessTokenProvider`
(client-credentials against the Knox token endpoint) wired into `RESTCatalogService`'s
`OAuth2 Access Token Provider`, `Catalog URI` = the datashare `iceberg-rest` endpoint,
namespace/table = `poc_uc2`/`airlines`. No dynamic S3 properties needed — credentials are vended.
