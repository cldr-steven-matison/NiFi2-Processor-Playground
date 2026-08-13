# NiFi 2.0 Processor Playground

A playground repository for experimenting with **custom Python processors** (and Java NAR processors) in **Apache NiFi 2.0** running on Cloudera Streaming Operators / Kubernetes.

Built to support the blog series on rapid custom processor development:
- [Custom Processors with Cloudera Streaming Operators](https://cldr-steven-matison.github.io/blog/Custom-Processors-With-Cloudera-Streaming-Operators/)
- [How to Build and Test Custom NiFi Processors with AI (Without Breaking NiFi)](https://cldr-steven-matison.github.io/blog/How-to-AI-with-NiFi-and-Python/) (2026-04-29)

---

## Purpose

End location for operational custom nifi processors.

---

## Repository Structure

- **`nifi-custom-processors/`**  
  All Python custom processors using the official `nifiapi` package.  
  See [`nifi-custom-processors/README.md`](./nifi-custom-processors/README.md) for details on each processor and development order.

- **`my-custom-nifi-bundle/`**  
  The Maven scaffold for a native **Java NAR** processor — generated straight from Apache's processor archetype. Start here when you need JVM speed, a controller service, or a first-class shipped processor type.

- **`nifi-geticeberg-bundle/`**  
  A worked native Java processor: **`GetIceberg`**, the read counterpart to the stock write-only `PutIceberg`. It plugs into the live `RESTCatalogService`, scans an Iceberg table, and emits the rows through a Record Writer — proven reading a CDP Data Share table end to end. See [`nifi-geticeberg-bundle/README.md`](./nifi-geticeberg-bundle/README.md).

- **Main supporting repo**  
  [ClouderaStreamingOperators](https://github.com/cldr-steven-matison/ClouderaStreamingOperators) — full Kubernetes manifests, NiFi CRDs, and deployment patterns.

---

## Two ways to build a processor

Python and Java are both first-class in NiFi 2.0. Pick by how you iterate and what the processor needs to reach:

| | Python processor | Java / NAR processor |
|---|---|---|
| Language | Python 3 | Java 21 |
| Build | none — drop a `.py` file | Maven (`nifi-processor-bundle-archetype`) |
| Base class | `FlowFileTransform` / `FlowFileSource` (`nifiapi`) | `AbstractProcessor` (`nifi-api`) |
| Delivery | mount / extensions volume | `kubectl cp` the NAR into the extensions autoload dir (or a PVC + `narProvider`) |
| Reload | hot-reload in 30–60 s | rebuild + copy; bump the bundle version, no hot-reload |
| Reach for it when | fast iteration, glue logic, ML in Python | performance, a controller service, a shipped type |

The Python path is the two blog posts above. The Java path is short once you've done it once:

1. **Scaffold** a bundle from the archetype (`my-custom-nifi-bundle/` is the output) — a processors module plus a `packaging=nar` module.
2. **Write the processor** by extending `AbstractProcessor`: declare `PropertyDescriptor`s, declare `success`/`failure` relationships, do the work in `onTrigger` through the `ProcessSession`. Register the fully-qualified class name in `META-INF/services/org.apache.nifi.processor.Processor` — miss this and the NAR loads but the processor never appears.
3. **Unit-test with `TestRunner`** (the `nifi-mock` dependency is already in the POM) — prove the type is wired before you ever deploy.
4. **Build the NAR** with `mvn clean install`.
5. **Deploy** by copying the NAR into NiFi's extensions autoload directory; iterate by bumping the bundle version and repointing the processor.

`nifi-geticeberg-bundle/` is the end-to-end worked example: it does all five, plugs a real `RESTCatalogService` controller service, and returns real rows from a live catalog. Its README covers the parts that only bite in the field — the parent-NAR classloader trick, extracting the CFM dependency jars, and the version-bump-to-redeploy rule.

---

## Quick Start (Python)

1. Clone this repo.
2. Follow the mounting instructions in the [Custom Processors blog](https://cldr-steven-matison.github.io/blog/Custom-Processors-With-Cloudera-Streaming-Operators/) (or the newer AI guide).
3. Drop any `.py` file from `nifi-custom-processors/` into your mounted extensions folder.
4. Wait 30–60 seconds → refresh the NiFi UI → drag the processor onto the canvas.

Hot-reload works automatically. Just edit, save, wait, refresh. For the Java NAR path, follow the five steps above and `nifi-geticeberg-bundle/README.md`.

## Contributing / Adding New Processors

Add Python processors to `nifi-custom-processors/`; add Java processors as their own `nifi-*-bundle/` NAR bundle alongside `nifi-geticeberg-bundle/`. PRs welcome.