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
  Maven-based Java processor bundle (NAR) examples for when you need native Java performance or deeper NiFi integration.

- **Main supporting repo**  
  [ClouderaStreamingOperators](https://github.com/cldr-steven-matison/ClouderaStreamingOperators) — full Kubernetes manifests, NiFi CRDs, and deployment patterns.

---

## Quick Start

1. Clone this repo.
2. Follow the mounting instructions in the [Custom Processors blog](https://cldr-steven-matison.github.io/blog/Custom-Processors-With-Cloudera-Streaming-Operators/) (or the newer AI guide).
3. Drop any `.py` file from `nifi-custom-processors/` into your mounted extensions folder.
4. Wait 30–60 seconds → refresh the NiFi UI → drag the processor onto the canvas.

Hot-reload works automatically. Just edit, save, wait, refresh.

## Contributing / Adding New Processors

New processors should be added to `nifi-custom-processors/` with PRs.