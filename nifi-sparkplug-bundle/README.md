# nifi-sparkplug-bundle — native Sparkplug B **publish** processor (`PublishSparkplug`)

Cloudera's IIoT NAR ships `ConsumeMQTTIIoT` — the *consume/decode* side of Sparkplug B — and
nothing for **publishing**. The stock `PublishMQTT` moves raw bytes and knows nothing about
Sparkplug. This bundle is that missing publish side: one native Java processor that encodes an
incoming FlowFile's metrics as a spec-compliant Sparkplug B payload (Eclipse Tahu) and publishes it
over MQTT (Eclipse Paho), managing the birth/death and sequence-number state machine the spec
requires.

It is built to run on a **MiNiFi Java** edge agent (side-loaded into `extensions/`, the same recipe
used for the CDF IIoT NAR) as well as on NiFi/CFM core — so an edge node can *originate* Sparkplug B
without an `InvokeHTTP` or `ExecuteScript` shim.

## `PublishSparkplug` — encode + publish

Input processor (`INPUT_REQUIRED`). One FlowFile in → one Sparkplug B **NDATA** message out on
`spBv1.0/<group>/NDATA/<node>`. The processor owns the full node-session lifecycle:

- **NBIRTH first.** On the first FlowFile of a session it publishes an **NBIRTH** certificate
  (`spBv1.0/<group>/NBIRTH/<node>`) declaring `bdSeq`, the `Node Control/Rebirth` control metric,
  and the metrics from that FlowFile — *before* any NDATA. A consumer that sees NDATA without a
  matching birth treats the node as stale.
- **NDEATH as the will.** The NDEATH certificate for the session is registered as the MQTT *will*
  before connecting, so an ungraceful drop still delivers a death; it is also re-published on a
  graceful `@OnStopped`.
- **Sequencing.** `bdSeq` (one per session, shared by that session's NBIRTH + NDEATH) and `seq`
  (0–255, wrapping, reset to 0 by each NBIRTH) are managed for you in `SparkplugPayloadFactory` —
  never hand-rolled. A publish failure drops the connection so the next FlowFile reconnects and
  re-births.

### Metric input format

FlowFile content is a flat JSON object of **metric name → value**; the JSON type sets the
Sparkplug data type:

```json
{
  "Sensors/Temperature": 22.5,   // decimal  -> Double
  "Sensors/Count":       1013,   // integral -> Int64
  "Sensors/Online":      true,   // boolean  -> Boolean
  "Sensors/Label":       "ok"    // text     -> String
}
```

The metric set of the FlowFile that opens a session defines the birth certificate. Metric names may
use `/` for the Sparkplug folder convention.

### Properties

| Property | Notes |
|---|---|
| `Broker URI` | `tcp://host:1883` / `ssl://host:8883` |
| `Client ID` | stable per agent |
| `Group ID` | Sparkplug `<group_id>` (default `FactoryLine1`) — supports EL |
| `Edge Node ID` | Sparkplug `<edge_node_id>` (default `Edge-01`) — supports EL |
| `Quality of Service` | `0` / `1` / `2` |
| `Username` | optional |
| `Password` | **sensitive** — bind to a Parameter Context (`#{mqtt-password}`), never a literal |

### Written attributes

`sparkplug.topic`, `sparkplug.message.type` (`NDATA`), `sparkplug.seq`, `sparkplug.bdSeq`,
`sparkplug.metric.count`, and — on the `failure` relationship — `sparkplug.error`. Two
relationships, `success` and `failure`, mirroring the stock IIoT/MQTT processors' surface.

## How it is put together

- **`PublishSparkplug`** — the `AbstractProcessor`: properties, relationships, session lifecycle,
  and the onTrigger orchestration (`parse → ensureSession(birth) → publish NDATA`). `@TriggerSerially`
  so the sequence counters advance single-threaded.
- **`SparkplugMetricParser`** — FlowFile JSON → `List<org.eclipse.tahu…Metric>`, with type inference.
- **`SparkplugPayloadFactory`** — builds/encodes NBIRTH/NDATA/NDEATH and owns the `bdSeq`/`seq`
  counters. This is where the spec's sequencing rules live; unit-tested in isolation.
- **`MqttPublisher`** (interface) + **`PahoMqttPublisher`** (impl) — the MQTT transport seam. The
  processor talks to the interface, so its Sparkplug logic is testable with an in-memory fake and
  the one class that opens a socket is isolated (and excluded from the coverage gate).

The NAR is **self-contained** — it bundles the Tahu / Paho / protobuf / Jackson closure and declares
no parent NAR, so it drops onto a stock MiNiFi Java agent with nothing to line up first (unlike the
iceberg bundle, which parents the CFM services-api NAR to reach a live controller service).

## Build and deploy

Requires **JDK 21+** (NiFi 2.6.0). Build:

```bash
mvn clean install -Denforcer.skip=true      # unit + TestRunner tests; JaCoCo report under target/site/jacoco
# mvn -Denforcer.skip=true clean verify      # also runs the 80% bundle line-coverage gate
```

Deploy onto a MiNiFi Java agent (or NiFi/CFM) by copying the NAR into the extensions autoload
directory:

```bash
kubectl cp -c <container> nifi-sparkplug-nar/target/nifi-sparkplug-nar-1.0.0-SNAPSHOT.nar \
  <ns>/<agent-pod>:/opt/minifi/minifi-current/extensions/
```

NiFi/MiNiFi hot-loads the NAR (no restart). Note NiFi will not re-register a same-version
overwrite: **bump the bundle version for every redeploy**, then point the processor at the new
bundle version. Side-load the *full* dependency closure at a matching version — one bad or
mismatched artifact fails the whole extension-load batch.

Enroll/redeploy the agent through EFM only — get the deployer command from EFM's *Deploy Agent* CLI
screen or `POST /efm/api/agent-deployer/generateCommand` (omit `agentIdentifier`); never hand-build
it or reuse an identifier across a fresh enrollment.

## Verify end-to-end

1. **On the wire** — `mosquitto_sub -h <broker> -p 1883 -v -t 'spBv1.0/#'`; expect a binary
   `…/NBIRTH/…` frame then repeating `…/NDATA/…` frames.
2. **Decode validates** — point a `ConsumeMQTTIIoT` flow at `spBv1.0/#` and confirm messages route
   via the **`Message`** relationship, **not** `parse.failure`. Message-not-parse.failure is the
   real test — the consumer's parser accepted the bytes as spec-compliant Sparkplug B.
3. If NDATA arrives but the consumer keeps requesting a rebirth, the NBIRTH is missing or the
   `bdSeq`/`seq` is off — recheck birth-first ordering and the counters.

## References

- [Eclipse Tahu](https://github.com/eclipse-tahu/tahu) — reference Sparkplug B implementation
- [Eclipse Paho](https://www.eclipse.org/paho/) — MQTT client
- [Sparkplug B specification](https://sparkplug.eclipse.org/)
