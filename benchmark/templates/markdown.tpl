# Benchmark Summary

- **Runs:** {{len .Runs}} (object counts: {{runCountsString .Runs}})
- **Baseline heap:** {{printf "%.0f" .Data.Baseline}} bytes ({{printf "%.2f" (div .Data.Baseline 1024)}} KiB, {{printf "%.4f" (div .Data.Baseline 1024 1024)}} MiB)
- **Per-object YAML:** {{printf "%.0f" .Data.YamlPerObj}} bytes ({{printf "%.2f" (div .Data.YamlPerObj 1024)}} KiB)
- **Per-object heap:** {{printf "%.0f" .Data.PerObj}} bytes ({{printf "%.2f" (div .Data.PerObj 1024)}} KiB)
- **Per-object diff (overhead):** {{printf "%.0f" .Data.Overhead}} bytes ({{printf "%.2f" (div .Data.Overhead 1024)}} KiB)

## Runs

| num_objects | heap_bytes | yaml_bytes | heap_KiB | yaml_KiB |
|-------------|------------|------------|----------|----------|
{{range .Runs}}| {{.N}} | {{printf "%.0f" .HeapBytes}} | {{printf "%.0f" .YAMLBytes}} | {{printf "%.2f" .HeapKiB}} | {{printf "%.2f" .YAMLKiB}} |
{{end}}

## Totals (sum across runs)

- **Total heap bytes (sum):** {{printf "%.0f" .TotalHeap}} ({{printf "%.2f" (div .TotalHeap 1024 1024)}} MiB)
- **Total YAML bytes (sum):** {{printf "%.0f" .TotalYAML}} ({{printf "%.2f" (div .TotalYAML 1024)}} KiB)

## Breakdown (derived estimates)

| metric | bytes | KiB | MiB |
|--------|-------|-----|-----|
| baseline_heap_bytes | {{printf "%.0f" .Data.Baseline}} | {{printf "%.2f" (div .Data.Baseline 1024)}} | {{printf "%.4f" (div .Data.Baseline 1024 1024)}} |
| per_object_yaml_bytes | {{printf "%.0f" .Data.YamlPerObj}} | {{printf "%.2f" (div .Data.YamlPerObj 1024)}} | {{printf "%.4f" (div .Data.YamlPerObj 1024 1024)}} |
| per_object_heap_bytes | {{printf "%.0f" .Data.PerObj}} | {{printf "%.2f" (div .Data.PerObj 1024)}} | {{printf "%.4f" (div .Data.PerObj 1024 1024)}} |
| per_object_diff_bytes | {{printf "%.0f" .Data.Overhead}} | {{printf "%.2f" (div .Data.Overhead 1024)}} | {{printf "%.4f" (div .Data.Overhead 1024 1024)}} |

## Charts

### Heap vs object count
```mermaid
xychart-beta
    title "Heap bytes vs number of objects"
    x-axis {{runCountsQuoted .Runs}}
    y-axis "heap bytes" 0 --> {{maxHeap .Runs}}
    line [{{heapLineValues .Runs}}]
```

### Heap bytes per run
```mermaid
xychart-beta
    title "Heap bytes by run (object count)"
    x-axis {{runCountsQuoted .Runs}}
    y-axis "heap bytes" 0 --> {{maxHeap .Runs}}
    bar [{{heapLineValues .Runs}}]
```

{{if .Pie}}### Memory breakdown (modeled for largest N)
```mermaid
pie showData
    title "Heap model for N={{.Pie.LastN}} objects"
    "baseline" : {{printf "%.1f" .Pie.BaselinePct}}
    "per-object" : {{printf "%.1f" .Pie.PerObjPct}}
```

{{end}}### YAML bytes vs object count
```mermaid
xychart-beta
    title "YAML bytes vs number of objects"
    x-axis {{runCountsQuoted .Runs}}
    y-axis "yaml bytes" 0 --> {{maxYAML .Runs}}
    line [{{yamlLineValues .Runs}}]
```
