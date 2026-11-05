# RecordReplay

Experimental standalone OTLP recorder and timestamp-paced replayer. Run it on a DVM or external host, not inside the ALDOTel cluster. Recording embeds a minimal Collector with the OTLP receiver and the existing [file exporter](../../exporter/fileexporter/README.md), rather than building the full Contrib distribution. There is no UI.

## Distribution and installation

RecordReplay is intended to be released in two forms: a standalone `recordreplay` executable for end users and an importable [Go library](#go-library-for-developers) for application developers. Binary release packaging is not implemented yet; the usage below assumes you have obtained a binary built for your operating system and architecture. Library registration in Contrib's release configuration does not mean a version has already been published.

Place the executable in a directory on your `PATH`, or invoke it by its full path. Running the binary does not require Go or a source checkout. Prepare a YAML configuration, any referenced TLS certificates/keys, and a writable output directory for recording or an existing recording for replay. Replace `/path/to/config.yaml` in the examples with your configuration path; the executable does not embed a default configuration.

You can run these commands from any working directory. Relative configuration paths, recording paths, and TLS file paths are resolved against that working directory, not the executable's location or the YAML file's directory. Use absolute paths when running from different directories or through automation.

## Record

With the installed executable:

```shell
recordreplay record --config /path/to/config.yaml
```

The example listens on `localhost:4317` for OTLP/gRPC and `localhost:4318` for OTLP/HTTP. Targets must explicitly export to this tool. To accept remote targets, configure an appropriate listen address and TLS/authentication as supported by the included components; no authentication extensions are included in this first draft.

The example writes traces, metrics, and logs to separate uncompressed, versioned recording files under `./recordings`, relative to the process working directory. Each record contains a write-time timestamp and a Protobuf telemetry batch. Keep signals separate because the recording does not identify their type. Future enhancements planned include adding Parquet exporters in order to make this recording compatible with systems that import and use Parquet-based telemetry files for offline analysis.

The importable [`pkg/recordreplay`](../../pkg/recordreplay/README.md) library (package `replay`) owns the [version 1 file format](#file-format), its record/header structures, frame writing and reading, boundary verification, and incomplete-tail truncation. Recording uses its header and frame writers through the file exporter's hooks; the exporter still owns buffering, locking, flushing, syncing, and file lifecycle. Replay uses the same library for framing, timestamp shifting, OTLP transport, and scheduling. Configuration providers, commands, the capture-duration timer, and human progress remain CLI-owned.

### Capture duration

Recording is bounded by `record.duration`, which defaults to **5 minutes** when omitted. Set a positive Go-style duration such as `30s`, `5m`, or `1h`; zero, negative, malformed durations, and unknown `record` fields are rejected before confirmation or output-file modification. Unlimited captures are not supported.

```yaml
record:
  duration: 5m
```

Override it through the existing Collector configuration mechanism:

```shell
recordreplay record --config /path/to/config.yaml --set record.duration=30s
```

The timer starts after startup preflight and any append/overwrite confirmation/repair succeed, immediately before Collector startup. Confirmation time is excluded; remaining startup time counts toward the duration. The timer runs even if no telemetry arrives. Startup output shows the chosen duration, and expiry prints a stop message and requests normal Collector shutdown: stop receiving, flush complete buffered records, sync when configured, and close files before returning successfully. Shutdown itself can take additional time; duration is not a hard process-exit or storage-completion deadline.

Standard input is read only for existing-file confirmation; Ctrl+C/SIGTERM and caller-context cancellation remain available for early graceful shutdown.

**If any configured output file already exists, recording refuses to start unless you select `--append` or `--overwrite` and confirm the modification.** Even empty files require permission. The options are mutually exclusive and apply to every configured file exporter, overriding its `append` setting, including values supplied using `--set`.

```shell
recordreplay record --config /path/to/config.yaml --append
recordreplay record --config /path/to/config.yaml --overwrite
```

`--append` retains the file header and complete existing records, including their timestamps, and writes new batches at the end; missing or empty files receive a new header. After confirmation and before the Collector starts, each existing nonempty recording's header is validated, then its record boundaries are checked. An incomplete final record's length prefix, timestamp, or payload triggers a warning identifying the file and retained byte count, and only that incomplete tail is truncated. The truncation is synced before appending. Clean recordings are not truncated. `--overwrite` instead truncates entire existing files and writes a new header, even if the config says `append: true`.

**This recording format is still under development, and not guaranteed to be backward compatible (yet)** Append rejects legacy files, unsupported versions/layouts, and incomplete or invalid file headers without modifying them. Use a fresh path or confirmed `--overwrite`; no migration or automatic deletion is performed. The file format is deliberately designed to be easy to extend through appending, and some efficiency sacrifices have been made in order to make that possible.

Before either operation starts, a warning lists the existing paths, explains the selected action (including incomplete-tail removal for append), and asks you to type `yes` and press Enter. No output file is opened until this confirmation is accepted. Any other answer, an input error, or EOF cancels recording with a nonzero exit status and leaves existing files untouched. There is no implicit confirmation from selecting a flag, and a config-only `append: true` does not bypass this check.

Tail verification requires existing recordings to be regular files; symlinks and special files are rejected rather than risking incorrect truncation. After validating the bounded file header, verification reads the four-byte record lengths and skips timestamps/payloads using file offsets, without deserializing telemetry or allocating payload-sized buffers. It adds startup work only, not recording-time checksums. This is a frame-boundary check, not an integrity check: it cannot detect same-length timestamp/payload corruption or reliably distinguish a corrupted length prefix from an interrupted write.

If no output file exists, recording starts without a confirmation prompt. Configuration validation remains read-only and does not prompt, inspect, or repair existing recordings. Versioned recordings require `format: proto` without compression, encoding extensions, or rotation; these restrictions apply even to fresh files and configuration validation. Stop the previous recorder before starting another against the same files; the startup check is not an interprocess lock.

The check uses the resolved configuration, including environment values and `--set` overrides. It is a startup preflight, not an interprocess lock: use a distinct recording directory per session and do not run concurrent recorders against the same paths. Duplicate paths across exporters and dynamic `group_by` paths are rejected by the recorder. These safeguards apply to RecordReplay, not other Collector executables using the file exporter.

Telemetry can contain sensitive data; restrict access to the output directory.

Use configuration files and the existing Collector `--set` overrides:

```shell
recordreplay record --config /path/to/config.yaml \
  --set 'receivers.otlp.protocols.grpc.endpoint=127.0.0.1:14317' \
  --set 'exporters.file/traces.path=./session-traces.pb' \
  --set 'exporters.file/metrics.path=./session-metrics.pb' \
  --set 'exporters.file/logs.path=./session-logs.pb'
```

Configuration supports file, environment, and inline YAML providers. Only OTLP receivers and file exporters are registered; processors, other exporters, connectors, and extensions are not included. Configuring unsupported components fails validation.

`config.yaml` is shared by recording and replay. Recording consumes the top-level `record` settings and removes them before Collector validation; it ignores the top-level `replay` section. Replay reads only its own section and does not start the recording receiver, open its configured output files, or run its preflight.

## Session progress

Both commands print startup details, progress every five seconds, and a final summary to standard output. Change the interval with `--progress-interval=1s`; `--progress-interval=0` disables periodic updates but retains startup/final summaries. Negative intervals are rejected. Validation and cancelled recording confirmation do not start progress reporting. The setting is a command-line flag, not a YAML field.

Recording reports elapsed time, session batch/payload-byte totals, average batches/bytes per second since startup, and time since the last successful write. It reports `waiting_for_data` even when no telemetry arrives. Counts include all configured signals, exclude pre-existing appended records, and count only successful complete frame writes. A batch is an OTLP payload, not an individual log, span, or metric data point. Payload bytes exclude the file header and frame envelope. Writes may still be buffered or unsynced; these are not durable-storage counters.

Replay startup identifies the input file, input size, first recorded write timestamp, selected signal, protocol, destination, TLS/server name, timestamp-rebasing mode, lateness tolerance, and request timeout without printing certificate/key contents or payloads. Periodic updates report acknowledged batches (`sent`), payload bytes, lifetime-average rates, backwards timestamps, maximum sender lateness, and time since the last successful response. The state distinguishes connection setup, reading/preparing a record, waiting for its absolute deadline, and an in-flight export. For a known record, updates include its number, recorded write timestamp, scheduled offset, and remaining wait. Reports continue during long gaps, connection setup, and blocked exports; a pending/failed export is not counted as successful. The final summary identifies completion versus stopping on error/cancellation.

Progress runs on a separate goroutine and snapshots small counters; it does not scan recordings, decode additional telemetry, flush/sync files, or shift replay deadlines. Standard-error warnings and standard-output progress are serialized if redirected to the same writer. Restart the command to use new progress code; an already running session is not changed.

## File format

The detailed [File format design reference](../../docs/design/recordreplay.md#file-format) defines the implemented version 1 header, record layout, timestamp semantics, and compatibility rules.

The structures in [`pkg/recordreplay/framing.go`](../../pkg/recordreplay/framing.go) are the format contract. `replay.Record` has exactly two fields:

```go
type Record struct {
    TimestampUnixNano int64
    Payload           []byte
}
```

The file header is `"OTELRR\r\n"` (eight magic bytes), followed by a four-byte unsigned big-endian JSON-header length, then UTF-8 JSON. That JSON contains `version: 1`, the record-framing definition, and an ordered `fields` array describing each field's name, encoding, and meaning. The header is written once when an empty file is opened, even if no telemetry arrives, and is not repeated on append. JSON headers are limited to 64 KiB before allocation.

Each record uses this binary layout; Go struct memory layout is not written to disk:

```text
[uint32 big-endian record length][int64 big-endian timestamp_unix_nano][payload bytes]
```

The length excludes its own four-byte prefix and includes the eight-byte timestamp, so an empty payload has length eight. The timestamp is signed, two's-complement UTC Unix nanoseconds captured when the exporter invokes the frame writer under its write lock. It is not the event timestamp inside the payload or the receiver's arrival timestamp, and wall-clock adjustments can make timestamps move backwards. Buffering and syncing happen separately; this is not a durability timestamp.

**Receiver-capture timestamping is planned, not implemented.** The intended boundary is receiver request handling, before the batch enters the pipeline, with that timestamp carried unchanged to the writer. It is not first-byte network arrival. This removes later processing/marshaling/lock delays from the timestamp source, but does not retroactively recover capture times from existing recordings. Changing the timestamp meaning requires updating the header definition and format version even if the physical record layout stays unchanged.

The payload is a reserialized OTLP Protobuf telemetry batch, not original gRPC framing or metadata. Event timestamps inside it remain unchanged. Reading the capture timestamp does not require decoding the payload. No signal identifier, transport, sequence, per-record version, or checksum is stored; the file-level version selects the record layout, and signal type comes from the configured file. Layout changes require a new version, which current readers reject explicitly rather than misinterpreting.

The frame reader distinguishes clean EOF from incomplete records and enforces a caller-selected payload-size limit before allocating. A valid header-only file contains zero records. File-header failures and structurally invalid record lengths are fatal and are never treated as repairable tails.

## Recording directory and durability

Each file exporter's `path` specifies its output filename, including the directory. Use `sync_interval` to configure durable syncing:

```yaml
exporters:
  file/logs:
    path: ./recordings/logs.pb
    create_directory: true
    format: proto
    flush_interval: 1s
    sync_interval: 5s
```

In `config.yaml`, each exporter has its own `path`, such as `./recordings/logs.pb`. Set these independently to choose the directory and filename for each telemetry type. Relative paths are resolved against the process working directory; absolute paths are also supported. `create_directory: true` creates missing parent directories.

`flush_interval` moves application-buffered bytes to the OS. A positive `sync_interval` additionally flushes the buffer and calls the underlying file's `Sync` at that interval, serializing it with writes. Shutdown flushes buffers and syncs before closing the file. Zero (the file exporter default) disables syncing; negative intervals are invalid. The example enables syncing every five seconds. Versioned recordings do not support rotation, even when syncing is disabled.

Background flush/sync failures are logged and returned on subsequent exports and shutdown rather than silently ignored. A sync interval is not a hard data-loss bound: scheduling, slow storage, and errors can delay successful syncs. A request can be acknowledged before its bytes are synced. File syncing does not sync parent-directory entries or promise durability beyond the filesystem/WSL/storage stack. Unflushed or unsynced data and a partially written final batch can be lost after a crash. Confirmed `--append` checks and removes an incomplete tail before resuming; the generic file exporter's `append: true` alone does not provide this recovery.

Validate without starting listeners or opening recording files:

```shell
recordreplay record validate --config /path/to/config.yaml
```

Recording stops automatically at `record.duration`; Ctrl+C also requests normal Collector shutdown and is not a force kill. Wait for the shutdown messages and final recording summary before restarting or replaying. Force termination (`SIGKILL`), another interrupt during shutdown, process/host crashes, request failures, or disk errors can still lose data; timed shutdown does not make recording lossless or crash-atomic.

## Replay

Set the destination, signal, and TLS settings in your configuration, then run the installed executable:

```shell
recordreplay replay --config /path/to/config.yaml
recordreplay replay /path/to/recordings/session/logs.pb --config /path/to/config.yaml
recordreplay replay /path/to/recordings/session/traces.pb --config /path/to/config.yaml \
  --set replay.signal=traces \
  --set replay.endpoint=localhost:14317
```

The first command uses `replay.file` from the configuration. The other commands select a recording explicitly. See [Development](#development) for source-checkout commands.

The positional file overrides `replay.file`; omitting it uses the configured file. `--config` defaults to `config.yaml`, accepts repeated configuration sources (merged in order), and supports the same file/environment/inline-YAML providers as recording. `--set replay.<field>=<value>` overrides replay settings after those sources, including nested settings such as `replay.tls.insecure=true`.

```yaml
replay:
  file: ./recordings/session/logs.pb
  signal: logs
  rebase_timestamps: true
  endpoint: localhost:14317
  protocol: grpc
  tls:
    insecure: true
  lateness_tolerance: 10ms
  timeout: 10s
  max_payload_size: 67108864
```

| Setting | Meaning |
|---|---|
| `file` | Version 1 recording, overridden by a positional file argument |
| `signal` | Required: `logs`, `metrics`, or `traces`; never inferred from the filename or payload |
| `rebase_timestamps` | Shift payload timestamps to the current session; default `false` for configurations that omit it, explicitly enabled in the sample |
| `endpoint` | Required: `host:port` for gRPC, or an HTTP(S) base URL for HTTP |
| `protocol` | `grpc` (default) or `http` (OTLP/HTTP Protobuf, not JSON) |
| `tls` | Collector TLS settings, including `ca_file`, client certificate/key, and server-name override; transport security is enabled by default |
| `lateness_tolerance` | Maximum sender lateness before failing, default `10ms`; nonnegative |
| `timeout` | Positive connection/request timeout, default `10s` |
| `max_payload_size` | Positive allocation/send limit in bytes, default 64 MiB |

Use an explicit `tls.insecure: true` for a plaintext gRPC destination or an `http://` URL. For HTTPS use `tls.insecure: false`. HTTP appends `/v1/logs`, `/v1/metrics`, or `/v1/traces` to the base URL; configure a base, not an already signal-specific URL. Redirects are rejected. Only run replay against the intended test endpoint; replaying logs/metrics/traces is real telemetry ingestion, not a dry run. The sample destination is local; change it if a recorder is already listening there to avoid recording the replay.

### Timing and failure behavior

The first record's timestamp defines recorded zero time. After connection setup and preparing the first request, establish a monotonic replay start and send each record at the absolute deadline `start + (record.timestamp - first.timestamp)`. There is no speed scaling, batching, queue, or application retry. Records are sent sequentially in file order; reading/preparing the next record and previous response latency count against its deadline, rather than shifting the schedule and accumulating drift.

**The sample enables `rebase_timestamps: true` to make historical captures usable in current-time dashboards.** After connection setup, immediately before preparing the first request, calculate one session-wide shift: `current Unix nanoseconds - first recorded write timestamp`. Apply it to every nonzero structured OTLP payload timestamp: log event/observed times; span start/end/event times; metric point/start times and exemplars for gauges, sums, histograms, exponential histograms, and summaries. Relative event spacing, span durations, metric intervals, and each event's original age relative to capture remain unchanged. Do not overwrite all events with the same per-request time. Missing (zero) timestamps remain unset, and timestamps in arbitrary bodies/attributes are not rewritten. Unrepresentable shifts or shifted timestamps stop replay rather than wrapping. Both gRPC and HTTP support rebasing; it changes only outgoing payloads, never the source file, recorded scheduling offsets, or the file-format version.

Set `--set replay.rebase_timestamps=false` to preserve original payload timestamps; configurations that omit the setting also retain that behavior. In this mode, sending now does not make events current-time events: a recent-time dashboard can omit historical data, so query the original event time range instead. Even with rebasing, originally stale/skewed events retain their age relative to capture. Successful OTLP responses confirm endpoint acceptance, not downstream storage or visibility in HyperDX. Selecting `signal: logs` does not replay the separate metrics/traces files.

Here is how the system deals with ambiguous timestamps in the recorded data:

1. If a record is more than `lateness_tolerance` late at sender invocation, stop before sending it and return a nonzero exit status. Report successful sends, backwards records, and maximum observed sender lateness. A request failure or OTLP partial-success/warning response also stops replay without retry; a failed/ambiguous request may already have reached the destination.

2. As the explicit exception to deadline failure, if a timestamp moves backwards relative to the previous record or precedes the first record's timestamp, warn and send it immediately in file order. Do not shift zero time; later records still use their original absolute deadlines. Equal timestamps share a deadline. Offsets exceeding `time.Duration` range are rejected rather than wrapping.

Timing is measured immediately before invoking the OTLP client, not at first network byte or destination receipt. OS scheduling, transport encoding, network latency, and receiver processing prevent a hard destination-arrival guarantee. The default 10ms tolerance is enforced at runtime; it is not a claim that every workload can sustain it. Version 1 recordings contain exporter-write timestamps, so replay reproduces those stored offsets, not unrecorded ingress timing.

Replay opens the source read-only and uses its size at startup; it does not follow appended records, repair tails, or migrate legacy files. As a best practice, finish recording before replay. A valid header-only file succeeds with zero records and no destination connection. Invalid headers or an incomplete first record fail before connecting. A later incomplete/corrupt record stops with an error after reporting the successfully replayed prefix; the source remains unchanged. Ctrl+C cancels waits and in-flight requests.

## Scope of this draft

Recording reuses the file exporter, with durable syncing and file-format hooks added in this checkout. It records decoded telemetry with original event timestamps plus an explicit exporter-write timestamp per batch, not original request bytes, ingress arrival offsets, or cross-signal sequence metadata. Replay supports one version 1 file and one signal per invocation over gRPC or HTTP. Cross-file scheduling, receiver-capture timestamping, Parquet writing, and conversion remain future work; see the [saved architecture decision and Parquet recovery design](../../docs/design/recordreplay.md).

An additional operation to convert versioned Protobuf recordings to Parquet is in scope as an optional final phase, after the core recording and replay work, if time permits. It is not implemented yet; conversion should preserve the recorded write timestamps, but cannot reconstruct ingress timing absent from the source recording.

## Go library for developers

Applications can import `github.com/open-telemetry/opentelemetry-collector-contrib/pkg/recordreplay` as package `replay` instead of launching a subprocess. The [library README](../../pkg/recordreplay/README.md) documents framing, inspection, OTLP senders, timestamp shifting, scheduling, and progress callbacks.

The library has no command-line interface and does not embed the recording Collector. Your application owns configuration, orchestration, context cancellation, and presentation. Recording integration can use the format writers with its own telemetry pipeline; replay integration can use `OpenInput`, `NewReader`, `NewSender`, and `Replay`. End users run your compiled application without needing this repository's source.

The library is registered in the `contrib-base` release group. Once a version containing it is published, developers can consume that version through Go modules. Until then, development uses a local module replacement; this is separate from distributing the standalone executable.

## Development

The commands in this section require a source checkout and the Go version specified in `go.mod`. From `cmd/recordreplay`, run directly from source:

```shell
go run . record --config config.yaml
go run . replay --config config.yaml
```

From the repository root, use `go -C cmd/recordreplay run . record --config config.yaml` or `go -C cmd/recordreplay run . replay --config config.yaml`, because the command has its own Go module. `-C` changes the process working directory to `cmd/recordreplay`, so relative configuration and recording paths are resolved there.

To build and test from `cmd/recordreplay`:

```shell
go build -o ../../bin/recordreplay .
make test
make lint
```

The resulting executable is `bin/recordreplay` under the repository root. It can be copied to another compatible host and run using the installed-binary commands above, with that host's configuration, certificates, and recording paths.
