num_objects,heap_bytes,yaml_bytes
{{range .Runs}}{{.N}},{{printf "%.0f" .HeapBytes}},{{printf "%.0f" .YAMLBytes}}
{{end}}

baseline_heap_bytes,per_object_yaml_bytes,per_object_heap_bytes,per_object_diff_bytes
{{printf "%.0f" .Data.Baseline}},{{printf "%.0f" .Data.YamlPerObj}},{{printf "%.0f" .Data.PerObj}},{{printf "%.0f" .Data.Overhead}}
