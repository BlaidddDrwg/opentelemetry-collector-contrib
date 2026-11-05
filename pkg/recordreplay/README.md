# RecordReplay library

Experimental generic recording/replay library, imported as `replay "github.com/open-telemetry/opentelemetry-collector-contrib/pkg/recordreplay"`. This separate Go module depends directly on Collector TLS configuration, pdata, and gRPC; it does not embed a Collector or depend directly on Cobra or configuration providers. The standalone [RecordReplay command](../../cmd/recordreplay/README.md) consumes this library.

## Input and framing

`OpenInput` opens an accessible regular file, rejecting symlinks and identity changes while opening, and returns its fixed size. Close the returned file when finished. `NewReader(file, size)` creates an independent bounded reader positioned after the version 1 header; a zero-byte file is also supported. Appends beyond the supplied size are not followed. This is a bounded snapshot, not a filesystem lock or a copy: callers must prevent concurrent modification of existing input bytes.

`Inspect(ctx, file, size, signal, limit)` streams frames and validates OTLP Protobuf payloads without modifying or exporting input. `Inspection` reports bytes, batches, actual log/span/data-point items, minimum/maximum recorded write timestamps, and their duration. Empty and header-only inputs are valid. Malformed headers, incomplete frames, oversized payloads, invalid selected-signal payloads, and unrepresentable duration/counts fail. Protobuf has no signal discriminator; the caller must select the correct signal.

`ReadHeader`, `ReadFrame`, `WriteHeader`, `WriteFrame`, and `WriteBatch` implement the version 1 format. `Record` contains `TimestampUnixNano int64` and `Payload []byte`; the signal remains external to the file. `Header`, `Field`, `NewHeader`, and the format constants expose the header contract. `VerifyRecording` checks frame boundaries without allocating payload-sized buffers; header failures wrap `ErrInvalidHeader` and must not trigger repair. `TruncateIncompleteRecording` is for explicitly confirmed recorder recovery only, never inspection or replay.

## Transport and scheduling

`DefaultConfig` supplies defaults; `Config.Validate` validates file/signal/endpoint/protocol and timing/size settings. Configuration-provider loading remains the caller's responsibility. `NewSender(ctx, cfg, prepareConnection)` returns `Sender{Prepare, Close}`. Always close it. gRPC connects with TLS readiness, no retries, and no remote service-config overrides. HTTP uses OTLP Protobuf, rejects redirects, validates bounded Protobuf responses, and optionally prewarms a TCP/TLS connection without exporting a probe request. Both transports expose OTLP partial responses as `*PartialError{Rejected, Message}`.

After `NewReader` and `ReadFrame` obtain the first record, call `Replay(ctx, reader, first, cfg, sender.Prepare, clock, schedule, warnings, onProgress)`. Handle empty inputs before constructing a sender or calling `Replay`. Zero-valued `Clock` uses `time.Now` and cancellation-aware `WaitDeadline`. Tests can supply deterministic functions.

A nil schedule preserves standalone behavior: the first frame defines the origin, preparation precedes the monotonic start, and `cfg.RebaseTimestamps` shifts payload event times to session time while preserving unset timestamps. Backwards frames send immediately in file order with warnings, without resetting the baseline. A nonnil `Schedule` supplies a common `Start`, `OriginUnixNano`, and `TimestampShift`; all deadlines are `Start + recorded timestamp - OriginUnixNano`. The supplied shift is authoritative, independent of each input's first timestamp and `cfg.RebaseTimestamps`. Negative or unrepresentable offsets fail. Backwards scheduled frames retain strict deadlines. Preparation and response latency do not shift either schedule. `TimestampShift` checks signed duration overflow.

`Replay` sends sequentially, stops on any failure, and never retries. Strict deadline failures return `*LatenessError{Record, Late, Tolerance}` before attempting that record. Generic errors retain useful diagnostic context; a managed application owns any redaction, process protocol, readiness coordination, terminal reports, and lifetime.

## Progress and accounting

`Progress` callbacks run synchronously with immutable value snapshots for `preparing`, `waiting`, `sending`, and `reading`. Before an export, `sending` includes the updated attempted count and unknown acceptance; another callback follows the result, including failed or ambiguous outcomes. Keep callbacks fast and synchronize any application-side reporting goroutine separately.

`Stats.Sent` is the successful whole-batch count for CLI compatibility. `Attempted`, `Accepted`, `Failed`, and `Ambiguous` count log records, spans, or metric data points. `Batches` and `PayloadBytes` include acknowledged whole or quantified partial responses. A valid positive rejection count assigns rejected items to failed and the remainder to accepted; transport failures, malformed responses, invalid rejection counts, and unquantified partial warnings assign attempted items to ambiguous, never accepted. These counters describe sender acknowledgements, not guaranteed durable delivery or downstream loss. `MaxLate` records maximum observed lateness; `Backwards` counts backwards frame timestamps.

## Local validation

From this module, use `GOWORK=off go test ./...`, `GOWORK=off go test -race ./...`, `GOWORK=off go vet ./...`, `GOWORK=off go build ./...`, and `GOWORK=off make lint`. The library is registered in the `contrib-base` module set in `versions.yaml`. The command requires that set's version and uses a local replacement for development. Registration prepares the library for the Contrib release process; it does not publish a release immediately.
