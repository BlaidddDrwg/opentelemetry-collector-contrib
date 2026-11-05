# Standalone Telemetry Record/Replay Tool

**Draft for review**

## Purpose and deployment

Build a standalone command-line tool to capture real telemetry workloads and replay them against ALDOTel or another OpenTelemetry endpoint. The tool will run on either the DVM or a host machine outside the ALDOTel cluster. It will **not run within the cluster** and will have **no UI or UI integration**. Configuration will use files with command-line overrides.

## What exists today

The OpenTelemetry Collector provides OTLP receivers, Protobuf decoding/encoding, and OTLP exporters. The Contrib file exporter can save telemetry as length-prefixed Protobuf with optional compression. However, it records decoded pipeline data rather than original ingress messages and does not preserve their arrival sequence and spacing. The existing OTLP JSON file receiver reads JSON recordings; it does not provide timing-faithful Protobuf replay. These are reusable building blocks, not a complete standalone record/replay solution.

## File format

The current standalone implementation uses a versioned timestamp/payload recording format, documented in the [File format design reference](recordreplay.md#file-format). That section defines the magic bytes, JSON file header with version and field definitions, binary record structure, timestamp units and measurement boundary, append/recovery rules, and compatibility behavior. Version 1 currently timestamps at exporter write time; moving timestamp capture to the receiver is planned, not implemented. The Parquet container below remains a longer-term proposal, not the current on-disk format.

Single-file replay is now implemented using offsets from the first record's timestamp, with configurable sender-lateness failure and immediate/warn handling for backwards timestamps. The shared `config.yaml` and the [Replay design reference](recordreplay.md#replay) define the current CLI, transport choices, and limits. Parquet replay and legacy-file compatibility mentioned in the longer-term plan below are not implemented.

## Why Parquet

Choose Parquet primarily for its published format, interoperability, existing tooling, and long-term maintainability, not just compression. Standard readers can inspect metadata and extract Protobuf payloads; OTLP decoding is still needed to interpret payload contents. We own the versioned recording schema and replay semantics, not a proprietary storage format. Use broadly supported features and test independent readers; measure storage efficiency rather than assume it.

## Proposed tool

**Record mode:** Expose an OTLP endpoint for targets exporting traces, metrics, and logs. Capture original Protobuf messages before pipeline transformations. Store one message per Parquet row: an opaque Protobuf payload plus signal, transport, sequence, and monotonic arrival-offset columns, with a versioned schema. Use lossless compression; do not flatten telemetry into columns or re-marshal payloads. Define a consistent arrival boundary across HTTP and gRPC.

**Replay mode:** Read Parquet rows and send their Protobuf payloads to a configured OTLP endpoint, normally ALDOTel. Reproduce the original message sequence and inter-message spacing across signals using captured offsets, not telemetry event timestamps. Preserve message boundaries and support original or current-session payload timestamps. The implemented versioned-Protobuf replayer supports `replay.rebase_timestamps` using one fixed session-wide shift; see [the current replay design](recordreplay.md#replay). Reconstruct valid OTLP transport requests; Parquet itself is not transmitted.

**Configuration and operation:** Use config files with CLI overrides for mode, endpoints, recording path, TLS/authentication, and storage limits. Emit diagnostics and completion summaries. A repository skill explains the design and, once the executable is available, starts recording or replay only on explicit request after validating configuration and execution outside the cluster.

## Implementation plan

1. **Compose the standalone tool:** Reuse Collector OTLP transport code and an existing Go Parquet library. Keep deployment and execution independent of the cluster.
2. **Implement recording:** Add ingress capture, timestamp/sequence assignment, and the Parquet schema/writer. Bound row-group buffering and finalize rotated segments for readability and crash recovery. Verify original-byte capture for HTTP first, then gRPC.
3. **Implement replay:** Add the Parquet reader, OTLP sender, and an absolute-deadline scheduler driven by sequence and arrival offsets, not physical row order. Support existing file-exporter Protobuf files as an untimed compatibility mode.
4. **Validate fidelity and scale:** Verify payload-byte and timestamp preservation, ordering, and send-time spacing. Measure storage efficiency and CPU/memory overhead; test all signals, compression, concurrency, incomplete segments, disk exhaustion, and destination backpressure.

## Acceptance and operational limits

A successful recording must contain all received messages; storage failures or overflow must explicitly mark it incomplete. Replay must report timing deviations and fail qualification if the agreed timing tolerance is exceeded, rather than silently slow down or alter spacing. Exact destination arrival timing cannot be guaranteed across network and operating-system scheduling variability; define the measurement boundary, tolerance, and supported workload before claiming complete time fidelity. No processor-state restoration or cluster/UI changes are required.
