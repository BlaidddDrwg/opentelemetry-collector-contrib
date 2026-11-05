# RecordReplay: standalone executable in Contrib

**Decision date:** October 28, 2026.

The [overall design](standalone-telemetry-record-replay.md) and its [editable Word copy](standalone-telemetry-record-replay.docx) live alongside this decision in Contrib.

## Decision

Introduce `cmd/recordreplay` as a standalone Contrib executable with `record` and `replay` subcommands. Reuse existing Collector components instead of changing the general-purpose `otelcontribcol` CLI or building its complete component set. Keep application code together under `cmd/recordreplay`.

The tool runs on the DVM or a host outside the ALDOTel cluster. It does not run inside the cluster and has no UI or UI integration. Configuration uses files and command-line overrides.

## First draft

Implement `record` by composing the existing OTLP receiver and Contrib file exporter using the Collector's configuration and lifecycle code. Register only those components and the required internal telemetry factory. Reuse the Collector's configuration validation, `--config`, `--set`, and graceful shutdown rather than implementing a second configuration system.

Use the existing file exporter with command-specific file-format hooks and separate files for traces, metrics, and logs. Store a timestamp and Protobuf payload per record, as specified in [File format](#file-format). The current implementation captures exporter-write time, not receiver intake time. This captures decoded telemetry, not byte-identical ingress requests, arrival-time offsets, or a global message sequence. Durable syncing and format hooks do not rename the exporter or change the defaults for other Collector configurations. This draft makes no lossless-capture or ingress-timing-fidelity guarantee.

Centralize the format contract, record/header structures, frame reader/writer, verifier, and incomplete-tail truncator in `cmd/recordreplay/rr_file.go`. Connect the recorder using `fileexporter.NewFactoryWithFileFormat`; keep buffering, locking, syncing, and file lifecycle in the exporter. Its ordinary factory retains existing framing and configuration behavior. Frame reading distinguishes clean EOF from incomplete records and enforces a caller-selected payload allocation limit.

Implement single-file timestamp-paced replay as described in [Replay](#replay). Keep the recording file format and recorder timestamp source unchanged.

Do not rename or remove `fileexporter`: existing users and configurations continue to work. A link from its documentation points to the standalone command.

## File format

**Implemented:** version 1, defined in [`cmd/recordreplay/rr_file.go`](../../cmd/recordreplay/rr_file.go). This section is the design reference for the on-disk contract. The receiver-intake timestamp described below is a planned change, not current behavior.

### File header

```text
[8-byte magic "OTELRR\r\n"][uint32 big-endian JSON-header length][UTF-8 JSON header]
```

The JSON-header length excludes the magic and its own four-byte prefix. Reject zero-length headers and headers exceeding 64 KiB before allocating. The JSON describes the file-level version, record framing, and ordered field names, encodings, and meanings. Version 1 writes:

```json
{
  "version": 1,
  "framing": "uint32 big-endian record byte length, excluding the length prefix",
  "fields": [
    {
      "name": "timestamp_unix_nano",
      "encoding": "int64 big-endian",
      "description": "UTC Unix nanoseconds captured at exporter write time; not ingress or event time"
    },
    {
      "name": "payload",
      "encoding": "remaining record bytes",
      "description": "OTLP Protobuf telemetry batch; signal type is determined by the file configuration"
    }
  ]
}
```

The example is formatted for readability; the writer emits compact JSON. The file-level version selects the record interpretation, so neither the version nor field definitions are repeated per record. Readers reject unsupported versions, framing, or field layouts rather than guessing.

### Record structure and encoding

The in-memory structure has exactly two fields:

```go
type recordingRecord struct {
    TimestampUnixNano int64
    Payload           []byte
}
```

Serialize fields explicitly; do not write Go struct memory, padding, or slice descriptors. Each record has this layout:

| Byte offset | Encoding | Meaning |
|---|---|---|
| 0 | Four-byte unsigned big-endian integer | Record byte length, excluding this prefix and including the timestamp |
| 4 | Eight-byte signed, two's-complement big-endian integer | UTC Unix-nanosecond timestamp |
| 12 | Remaining record bytes | OTLP Protobuf telemetry batch |

An empty payload is valid and has a record length of eight. The maximum representable payload length is `uint32_max - 8`; readers additionally enforce a caller-selected payload-size limit before allocation. Reading the timestamp does not require decoding the Protobuf payload.

The payload is decoded telemetry reserialized as OTLP Protobuf, not original gRPC framing, headers, or other transport bytes. Event timestamps inside it remain unchanged. Signal type comes from the configured file; traces, metrics, and logs must remain in separate files. There is no signal, transport, sequence, checksum, footer, or per-record version field.

### Timestamp semantics

**Current version 1:** `writeRecordingBatch` samples `time.Now().UnixNano()` when the exporter invokes the frame writer under its write lock, after upstream handling and Protobuf marshaling. This is exporter-write time, not receiver capture time, telemetry event time, flush time, or durable-storage completion time. Processing, marshaling, and lock delays can distort the spacing compared with requests arriving at the receiver. Wall-clock adjustments can also make timestamps move backwards; this is not a monotonic arrival-offset clock.

**Planned receiver-capture behavior:** sample the timestamp at the receiver's request-handling boundary, before handing the batch into the pipeline, and carry it unchanged with that batch to the writer. This boundary is not first-byte network arrival and may follow transport decoding. Do not derive it from telemetry event timestamps or replace it with a later exporter, flush, or sync timestamp. Receiver-side timestamping and propagation are not implemented yet.

The record's physical timestamp/payload layout can stay the same when timestamping moves upstream, but its meaning changes. Update the file-header definition and format version for that semantic change so existing exporter-write timestamps cannot silently be interpreted as receiver-capture timestamps. Do not relabel or reconstruct capture times in existing version 1 files.

### File lifecycle and compatibility

Write the file header once when opening an empty file, including a session with no telemetry; a valid header-only file contains zero records. Retain the header and existing record timestamps on append. Confirmed overwrite truncates the file and writes a new header.

Earlier unversioned recordings are development artifacts, not version 1 files. Do not migrate them or mix formats through append; require a fresh path or confirmed overwrite without deleting files automatically. Reject unsupported versions/layouts and invalid or incomplete file headers without truncation. After a valid file header, an incomplete trailing record can be removed at the last complete boundary as described in [Recording safety and durability](#recording-safety-and-durability). Structurally invalid record lengths are fatal, not recoverable tails.

## Recording safety and durability

Before any recording output is opened, check all configured file-exporter paths after config resolution and CLI overrides. Refuse to start if any path exists, including an empty file, unless the user selects `--append` or `--overwrite` and explicitly confirms the modification. The flags are mutually exclusive: `--append` forces append mode for all file exporters, while `--overwrite` forces truncation, overriding config and `--set` values for `append`. Missing files are created in either mode.

List all existing paths, explain the selected action (including incomplete-tail removal for append), and require the user to type `yes` and press Enter before any output is opened. Selecting a flag is not itself confirmation. A different answer, input error, or EOF cancels without modifying existing files. If all paths are new, no confirmation is needed. A config-only `append: true` does not grant permission to modify existing files. Validation is read-only and noninteractive, but validates the selected mode. This is a startup check, not an interprocess lock; stop any previous recorder before reusing its files. Reject duplicate output paths and dynamic `group_by` paths in the recorder because their files cannot all be checked up front.

After append confirmation and before the Collector starts, validate each nonempty file's versioned header, then walk its four-byte big-endian record lengths, skipping timestamps/payloads with file offsets. Return the last complete frame boundary and distinguish incomplete record lengths, timestamps, or payloads from other failures. Use a separate truncation function to remove only bytes beyond that boundary and sync the repair; clean files are left intact. Invalid/incomplete file headers, unsupported versions/layouts, and structurally invalid record lengths must abort append without truncation. Empty files may be initialized with a fresh header. Report each repair and abort startup on checking, truncation, or sync errors. Existing append targets must be regular files; reject symlinks and special files. Require `format: proto`, no compression, no encoding extension, and no rotation for all versioned recordings, including fresh files and validation. Verification does not deserialize telemetry or allocate payload-sized buffers; no recording-time checksum is added. Boundary verification does not detect same-length timestamp/payload corruption or reliably distinguish corrupted record lengths from interrupted writes.

Use each file exporter's existing `path` to specify its output directory and filename independently. The supplied YAML uses `./recordings/traces.pb`, `./recordings/metrics.pb`, and `./recordings/logs.pb`, with directory creation enabled. Relative paths resolve against the process working directory; absolute paths are supported. No separate base-directory setting is needed.

Add `sync_interval` to the file-exporter config. A positive interval flushes buffers and calls the underlying file's `Sync` while holding the write lock; graceful close finalizes buffers/compression, syncs, then closes. Keep the existing `flush_interval` separate. Zero disables syncing for backward compatibility. The recording example uses five seconds. Report background flush/sync errors in diagnostics and subsequent exports/shutdown. Reject syncing with rotation until the rotating writer supports syncing the correct active file. File syncing alone does not ensure durable directory creation or naming, and all guarantees depend on the underlying storage honoring sync operations.

Unflushed/unsynced data loss is acceptable after a crash. There is no promise that an acknowledged export is already durable. A crash can also interrupt a length-prefixed Protobuf batch; confirmed append retains complete frames and removes an incomplete tail, but does not recover the lost batch or guarantee payload integrity. Replay sends a complete readable prefix and fails on an incomplete tail without modifying the source.

## Replay

**Implemented:** `replay [file] --config config.yaml`, with optional `--set replay.<field>=<value>` overrides. Rename the shared sample from `record.yaml` to `config.yaml`; retain its recording paths. The `replay` section specifies `file`, `signal`, `endpoint`, `protocol`, `tls`, `lateness_tolerance`, `timeout`, and `max_payload_size`. A positional file overrides `replay.file`. Signal is explicit because version 1 has no signal field. Recording strips the replay-only section before Collector validation; replay reads only that section and never starts the recorder.

Use `rr_file.go` to validate the header and stream bounded records from a read-only file whose input size is fixed at startup. Replay one signal from one version 1 file per invocation. Reuse the Collector pdata OTLP gRPC clients and TLS configuration, and send the stored Protobuf body directly for OTLP/HTTP; validate payloads before their scheduled sends. Do not introduce a replay receiver/plugin or full Collector pipeline for this single-file path: those would add lifecycle and queueing complexity without improving scheduling. No file-format changes are needed.

After gRPC connection readiness (or HTTP client setup) and first-request preparation, set a monotonic `start`. The first recorded timestamp is zero; each record's deadline is `start + (timestamp - first_timestamp)`. Preserve file order. Await each export response, but never schedule the next send relative to the previous completion; reading, preparation, response latency, and scheduler wakeup delay count toward its original deadline. Fail before sending a record more than the nonnegative `lateness_tolerance` late, default 10ms. A slow destination or dense workload may therefore fail instead of silently stretching replay duration. Report successful sends, backwards records, and maximum sender lateness, including a failed deadline.

`replay.rebase_timestamps` enables current-time payload timestamps; it defaults to false when omitted and is explicitly true in the shared sample. After connection setup and before first-request preparation, calculate one fixed `current Unix nanoseconds - first recorded write timestamp` shift for the session. Add it to nonzero log event/observed timestamps; span start/end/event timestamps; and all metric point/start/exemplar timestamps. Preserve event spacing, durations, intervals, zero/unset fields, arbitrary bodies/attributes, and event age relative to capture. Reject unrepresentable offsets or shifted values instead of wrapping. Mutate decoded outgoing requests only; never change the source file, header/version, or scheduling timestamps. gRPC shifts the already decoded request, while HTTP re-encodes only when a nonzero shift is needed. With rebasing disabled, preserve original event timestamps and HTTP payload bytes.

The user's selected exception is a backwards timestamp: warn and send immediately if it decreases from the preceding timestamp or precedes zero time, even if its original deadline would already be missed. Do not rebase later deadlines. Equal timestamps share deadlines. Reject unrepresentable offsets. There is no speed scaling, background sending queue, batching, or application retry. Request/connection timeout defaults to 10 seconds; allocation/send limit defaults to 64 MiB. Stop on transport errors, unexpected HTTP status/content type, invalid responses, and OTLP partial-success/warning responses; failed/ambiguous requests may already have been delivered.

Timing qualification measures invocation of the OTLP sender, not first-byte network departure or destination arrival. HTTP connection establishment may occur during its first request, and transport serialization/network/receiver delay remain outside this measurement boundary. Version 1 supplies exporter-write timestamps, not ingress capture timestamps. Receiver timestamp propagation remains planned, and replay cannot recover absent ingress timing or cross-signal order. Runtime tolerance checks and deterministic scheduler tests do not imply hard real-time guarantees under arbitrary load.

Replay never repairs or writes its input. Header-only files complete without connecting; invalid headers/first frames fail before connection. Later truncated or invalid records fail after reporting the successful prefix. Cancellation interrupts waits and requests. Tests cover absolute-deadline behavior without accumulated drift, lateness failure, backwards timestamps, cancellation, overflow, read-only/truncated inputs, shared config/overrides, and isolated gRPC/HTTP replay of logs, metrics, and traces with preserved event timestamps and measured receiver spacing.

## Session progress

Recording is bounded by the positive `record.duration` setting, defaulting to five minutes when omitted and overridable through `--set record.duration=30s`. Reject zero/negative/malformed values and unknown `record` fields before modification or confirmation. Consume and remove the `record` section before Collector validation. After preflight and any confirmation/repair succeed, start a per-session timer immediately before Collector startup; prompt time is excluded but remaining startup time counts. Expiry works even without traffic and cancels the Collector context rather than killing the process, so normal receiver/exporter shutdown flushes, optionally syncs, and closes recordings before returning successfully. Stop and join the timer on every exit path; shutdown can extend beyond the configured capture duration. There is no keyboard monitor, terminal-mode change, or platform-specific capture control. Standard input is used only for existing-file confirmation; Ctrl+C/SIGTERM and caller cancellation remain graceful early-stop options. This does not protect against force kills, crashes, or storage errors.

Record and replay print startup details, periodic five-second progress, and a final summary. A shared `--progress-interval` CLI flag changes the period; zero disables only periodic output and negative values fail before starting. Recording begins reporting only after successful preflight/confirmation, including sessions with no data; validation stays silent. Aggregate counters cover successful frame writes in the current session, not existing append data, event counts, or durable-storage completion.

Replay exposes the configured destination/signal/transport/TLS identity, first recorded write time, and selected timestamp-rebasing mode at startup. Periodic snapshots include acknowledged batch/payload-byte counts, lifetime-average rates, time since last success, backwards/max-lateness counters, and connection/read/prepare/deadline-wait/in-flight-export state. Known pending records include number, recorded write timestamp, scheduled offset, and remaining delay. Reporting continues while waiting or exporting, and failed/in-flight exports are not counted as acknowledged. Successful endpoint acceptance does not prove downstream persistence or UI visibility. Without rebasing, event timestamps remain historical and selecting a recent-time UI window can hide replayed data.

A per-session goroutine formats snapshots rather than adding terminal output to each batch's critical path. Counter updates require only small synchronized in-memory operations; progress does not inspect payloads, change the file format, force flushes/syncs, or alter replay scheduling/failure policy. Reporting stops before the final summary and command return.

## Target recording and replay

The intended recording container remains Parquet, with original OTLP Protobuf message bytes stored as opaque binary payloads alongside signal, transport, sequence, and monotonic arrival-offset columns. Do not flatten telemetry fields or re-marshal original payloads during capture.

Parquet is selected primarily for its published format, interoperability, existing tools, and long-term maintainability, not just compression. Standard Parquet readers can inspect metadata and extract payloads; interpreting payload contents still requires OTLP decoding. We retain responsibility for documenting and versioning the recording schema and replay semantics. Use broadly supported Parquet features and test independent readers.

Replay sends OTLP requests to a configured endpoint, usually ALDOTel. Select original or current-session payload timestamps with `rebase_timestamps` as described above. Reproduce captured ordering and spacing using an absolute-deadline scheduler, with a defined measurement boundary, timing tolerance, and explicit lag reporting.

The current file exporter cannot provide those future guarantees by itself. A subsequent design must establish the ingress capture boundary, payload ownership, bounded buffering, Parquet segmentation, and request-order metadata before claiming full record/replay fidelity. No Parquet dependency is needed for this first draft.

## Deferred scope: convert existing recordings to Parquet

Add a conversion operation to `cmd/recordreplay` after the core recording and replay work, only if time permits. This is an additional capability, not a prerequisite for recording or replay, and is not implemented yet.

Read versioned Protobuf recordings incrementally and write their payloads as opaque binary values in a versioned Parquet schema. Identify the signal from the supplied input mapping because the current recordings use separate traces, metrics, and logs files. Preserve capture write timestamps, payload bytes, event timestamps, and record order within each source file; do not flatten telemetry or rebase timestamps.

Current recordings contain exporter-write timestamps, not ingress arrival offsets, transport metadata, or a global cross-signal sequence. Conversion must mark unavailable metadata explicitly and must not invent ingress-timing fidelity or infer cross-signal arrival order from telemetry timestamps. Keep source files unchanged, report incomplete trailing records, and finalize and validate the Parquet output before publishing it. Define the conversion CLI and detailed schema when this deferred phase starts.

## Parquet crash recovery (planned, not implemented)

An unencrypted Parquet file ends with serialized file metadata, its four-byte length, and the magic bytes `PAR1`. The footer describes column-chunk locations and row groups; adding `PAR1` alone cannot make an interrupted file readable. See the Apache Parquet format specification: `https://parquet.apache.org/docs/file-format/`.

We cannot ensure a process closes a file after a forced kill, WSL termination, power loss, or host failure. Graceful shutdown is useful but is not the crash-recovery mechanism. Do not write an entire long recording into one indefinitely open Parquet file.

Instead, make a recording directory contain immutable, independently readable Parquet segments, with configurable maximum segment duration and size. Close segments on elapsed time even under low traffic so an active recording is not left unfinalized indefinitely. Store the schema version, session identifier, global sequence, and arrival offsets in each segment so finalized files remain discoverable without a single indispensable manifest.

For each segment, write a uniquely named `.parquet.partial` file, finalize the Parquet writer to emit its complete footer, sync the still-open underlying file, close it, then publish the final `.parquet` name without replacing an existing segment. On filesystems that support it, sync the containing directory after creation/publication and ensure newly created directory entries are persisted. Do not publish a segment until all these operations succeed. Sequence numbers and offsets continue across segments; file boundaries must not change replay timing.

On recovery, validate and retain finalized segments. Inspect partial files read-only: a fully finalized footer may be present if the crash happened before publication, in which case validation can permit publishing that segment. A missing or corrupt footer means the active segment is not a normal readable Parquet file. Quarantine it, preserve it for optional future salvage, and report the recording as incomplete; never silently skip the loss or destroy the original partial file. Appending a footer would require reconstructing accurate metadata, not just adding the magic bytes, and is not assumed to be available.

This design limits the unfinalized-file risk to the active segment, plus any data not successfully synced/published; segment duration is a target, not a hard bound under stalled I/O or errors. Periodic fsync of an open Parquet file alone does not make it readable. A durable per-message journal could recover the active segment by rebuilding it, but adds a second format/write path and is out of scope unless losing the active segment is unacceptable.

Before implementing Parquet, test forced termination during data writes, footer finalization, sync, and publication; test recovery with an independent reader and verify that all previously committed segments remain readable. Also test disk-full/sync errors and distinguish process-crash tests from host/WSL/storage-failure testing.
