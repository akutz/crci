# crci

**crci** is a CLI that benchmarks controller-runtime cache memory. It runs a minimal controller under [envtest](https://pkg.go.dev/sigs.k8s.io/controller-runtime/pkg/envtest), creates N resources with fuzz-style data, waits for cache sync, and reports heap usage and YAML size. From multiple runs (different N), it derives baseline heap, per-object heap, per-object YAML size, and overhead. Use it to estimate cache footprint for a given resource type (e.g. ConfigMap, a CRD).

## What it does

- Starts a local Kubernetes API server (envtest) and optionally loads CRDs.
- Registers a no-op controller that watches the resource type you specify.
- For each of several object counts (a Fibonacci sequence, e.g. with default `-fib 14`: 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610), creates that many resources with generated data, waits for cache sync, runs GC, and records heap bytes and total YAML size.
- Fits a simple model: `heap ≈ baseline + N × per_object_heap`, and reports baseline, per-object heap, per-object YAML, and per-object diff (overhead).
- Outputs results in **text** (default), **CSV**, or **Markdown** (with Mermaid charts).

## Usage

Build and run from the repo root:

```bash
go build -o crci .
./crci [flags]
```

Or run directly:

```bash
go run . [flags]
```

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-resource` | `configmap` | Resource to test: `configmap`, `secret`, `pod`, or `group/version/kind` (e.g. `stable.example.com/v1/CronTab`). |
| `-crds` | (none) | Comma-separated paths to CRD YAML files to load into envtest. Use when benchmarking a custom resource. Paths are relative to the module root. |
| `-format` | `text` | Output format: `text`, `csv`, or `markdown`. |
| `-fib` | `14` | Number of Fibonacci numbers to use as object counts (must be ≥ 2). With 14 you get runs for 1, 2, 3, 5, 8, … 610 objects. |
| `-fib-print` | `false` | If true, print the Fibonacci sequence to the console and exit (useful to see the N values for a given `-fib`). |

### Examples

```bash
# Default: configmap, text output
./crci

# Secret or Pod
./crci -resource=secret
./crci -resource=pod

# CSV for scripting
./crci -format=csv

# Markdown with summary and Mermaid charts
./crci -format=markdown

# Custom resource (CRD); provide CRD YAML so envtest can serve the API
./crci -resource=stable.example.com/v1/CronTab -crds=stable.example.com_crontabs.yaml

# Use fewer runs (first 8 Fibonacci numbers: 1, 2, 3, 5, 8, 13, 21, 34)
./crci -fib=8

# Print the Fibonacci sequence for default -fib and exit
./crci -fib-print
```

The first run may download envtest binaries (e.g. `kube-apiserver`, `etcd`).

## Output formats

- **text** – Human-readable baseline, per-object YAML, per-object heap, and per-object diff (bytes, KiB, MiB).
- **csv** – Two tables: (1) per-run `num_objects`, `heap_bytes`, `yaml_bytes`; (2) one row with `baseline_heap_bytes`, `per_object_yaml_bytes`, `per_object_heap_bytes`, `per_object_diff_bytes`.
- **markdown** – Summary, runs table, totals, breakdown table, and Mermaid charts (heap vs N, bar chart, pie breakdown, YAML vs N).

## Requirements

- Go 1.24+ (or as required by `go.mod`).
- controller-runtime and envtest dependencies (see `go.mod`).
