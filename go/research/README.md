# Research scripts

Standalone Go programs for investigating internal multigres behaviour —
wire-size measurements, proto/struct introspection, microbenchmarks of
internal helpers, and other one-off exploratory work. These are **not**
production code and are **not** built by `make build`.

## What goes here

- Programs that need to import internal multigres packages
  (`go/pb/...`, `go/services/...`, `go/common/...`) to construct or
  measure protocol artefacts.
- One-off measurement scripts, e.g. "how big is a health snapshot on
  the wire," "how many bytes does a 50-pooler topology serialize to."
- Throwaway repros for bugs, perf investigations, or protocol-design
  questions.

## What does NOT go here

- Production binaries → `go/cmd/<service>/`
- Reusable libraries → `go/common/...` (multigres-specific) or
  `go/tools/...` (generic helpers; cannot import multigres internals
  per `depguard`).
- Test fixtures or test helpers → `go/test/...` or `_test.go` files in
  the relevant package.
- Shell or Python scripts → `scripts/`.

## Layout

One subdirectory per program. Each subdirectory is a `package main`
with a single `main.go`:

```
go/research/
├── README.md
└── <topic>/
    └── main.go
```

## Running

From the repository root:

```sh
go run ./go/research/<topic>
```

Each program prints its output to stdout. Programs are expected to
exit cleanly on their own; long-running services don't belong here.

## Available programs

### `snapshot_size`

Constructs representative `ManagerHealthSnapshot` messages — primary
heartbeat, standby heartbeat, and a minimal uninitialized snapshot —
and prints each one's `proto.Marshal` wire size. Useful for sanity-
checking the orchestrator's per-pooler health-stream bandwidth.

```sh
go run ./go/research/snapshot_size
```

## Adding a new script

1. Create `go/research/<topic>/main.go` with `package main`.
2. Add a short entry to the "Available programs" section above
   describing what the script measures and how to interpret its
   output.
3. Commit as a single change so the script and its README entry land
   together.
