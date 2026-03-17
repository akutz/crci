baseline heap:
  bytes: {{printf "%.0f" .Data.Baseline}}
  kib: {{printf "%.2f" (div .Data.Baseline 1024)}}
  mib: {{printf "%.4f" (div .Data.Baseline 1024 1024)}}

per-object yaml:
  bytes: {{printf "%.0f" .Data.YamlPerObj}}
  kib: {{printf "%.2f" (div .Data.YamlPerObj 1024)}}
  mib: {{printf "%.4f" (div .Data.YamlPerObj 1024 1024)}}

per-object heap:
  bytes: {{printf "%.0f" .Data.PerObj}}
  kib: {{printf "%.2f" (div .Data.PerObj 1024)}}
  mib: {{printf "%.4f" (div .Data.PerObj 1024 1024)}}

per-object diff:
  bytes: {{printf "%.0f" .Data.Overhead}}
  kib: {{printf "%.2f" (div .Data.Overhead 1024)}}
  mib: {{printf "%.4f" (div .Data.Overhead 1024 1024)}}
