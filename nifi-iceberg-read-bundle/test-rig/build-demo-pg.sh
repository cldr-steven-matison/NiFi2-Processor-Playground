#!/usr/bin/env bash
# Builds the GetIcebergDemo process group on a single-user-auth NiFi:
#   RESTCatalogService (no OAuth) + JsonRecordSetWriter + GetIceberg -> funnel.
# Strictly additive: new PG at root, nothing existing is touched.
#
# Usage: build-demo-pg.sh <bearer-token> [catalog-uri] [warehouse]
# Defaults target the iceberg-rest-rig in the iceberg-demo namespace.
set -e
B="https://mynifi-web.cfm-streaming.svc.cluster.local:8443/nifi-api"
TOKEN="$1"
CATALOG_URI="${2:-http://iceberg-rest.iceberg-demo.svc.cluster.local:8181}"
WAREHOUSE="${3:-s3://warehouse/}"
AUTH="Authorization: Bearer $TOKEN"
j() { jq -r "$1"; }

ROOT=$(curl -sk -H "$AUTH" "$B/flow/process-groups/root" | j ".processGroupFlow.id")
echo "ROOT=$ROOT"

PG=$(curl -sk -H "$AUTH" -X POST "$B/process-groups/$ROOT/process-groups" -H 'Content-Type: application/json' \
  -d '{"revision":{"version":0},"component":{"name":"GetIcebergDemo","position":{"x":-1200.0,"y":-1200.0}}}' | j ".component.id")
echo "PG=$PG"

RC=$(curl -sk -H "$AUTH" -X POST "$B/process-groups/$PG/controller-services" -H 'Content-Type: application/json' \
  -d "{\"revision\":{\"version\":0},\"component\":{\"type\":\"com.cloudera.nifi.services.iceberg.RESTCatalogService\",\"name\":\"DemoRestCatalog\",\"properties\":{\"Catalog URI\":\"$CATALOG_URI\",\"warehouse-path\":\"$WAREHOUSE\"}}}" | j ".component.id")
echo "RC=$RC"

JW=$(curl -sk -H "$AUTH" -X POST "$B/process-groups/$PG/controller-services" -H 'Content-Type: application/json' \
  -d '{"revision":{"version":0},"component":{"type":"org.apache.nifi.json.JsonRecordSetWriter","name":"DemoJsonWriter","properties":{"Pretty Print JSON":"true"}}}' | j ".component.id")
echo "JW=$JW"

# Dynamic properties pass object-store specifics straight to the Iceberg client — needed
# because the tabulario/iceberg-rest fixture does not vend io-impl/S3 config to clients.
# Against a CDP datashare catalog (which vends credentials) these are unnecessary.
GI=$(curl -sk -H "$AUTH" -X POST "$B/process-groups/$PG/processors" -H 'Content-Type: application/json' \
  -d "{\"revision\":{\"version\":0},\"component\":{\"type\":\"org.apache.nifi.processors.iceberg.GetIceberg\",\"bundle\":{\"group\":\"com.example\",\"artifact\":\"nifi-iceberg-read-nar\",\"version\":\"1.0.1-SNAPSHOT\"},\"name\":\"GetIceberg\",\"position\":{\"x\":0.0,\"y\":0.0},\"config\":{\"schedulingPeriod\":\"1 min\",\"properties\":{\"catalog-service\":\"$RC\",\"catalog-namespace\":\"demo\",\"table-name\":\"airlines\",\"record-writer\":\"$JW\",\"io-impl\":\"org.apache.iceberg.aws.s3.S3FileIO\",\"s3.endpoint\":\"http://minio.iceberg-demo.svc.cluster.local:9000\",\"s3.path-style-access\":\"true\",\"s3.access-key-id\":\"admin\",\"s3.secret-access-key\":\"password\",\"client.region\":\"us-east-1\"}}}}" | j ".component.id")
echo "GI=$GI"

FUNNEL=$(curl -sk -H "$AUTH" -X POST "$B/process-groups/$PG/funnels" -H 'Content-Type: application/json' \
  -d '{"revision":{"version":0},"component":{"position":{"x":200.0,"y":300.0}}}' | j ".component.id")
echo "FUNNEL=$FUNNEL"

CONN=$(curl -sk -H "$AUTH" -X POST "$B/process-groups/$PG/connections" -H 'Content-Type: application/json' \
  -d "{\"revision\":{\"version\":0},\"component\":{\"source\":{\"id\":\"$GI\",\"groupId\":\"$PG\",\"type\":\"PROCESSOR\"},\"destination\":{\"id\":\"$FUNNEL\",\"groupId\":\"$PG\",\"type\":\"FUNNEL\"},\"selectedRelationships\":[\"success\"]}}" | j ".component.id")
echo "CONN=$CONN"

for CS in "$RC" "$JW"; do
  REV=$(curl -sk -H "$AUTH" "$B/controller-services/$CS" | j ".revision.version")
  curl -sk -H "$AUTH" -X PUT "$B/controller-services/$CS/run-status" -H 'Content-Type: application/json' \
    -d "{\"revision\":{\"version\":$REV},\"state\":\"ENABLED\"}" | j '.component.state'
done
