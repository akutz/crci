package benchmark

import (
	"encoding/base64"
	"fmt"
	"math"
	"math/rand"
	"net"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	openapispec "k8s.io/kube-openapi/pkg/validation/spec"
)

// fuzzConfigMap returns a ConfigMap with deterministic but varied data to
// simulate fuzz-loaded resources.
func fuzzConfigMap(namespace string, seed int) *corev1.ConfigMap {
	r := rand.New(rand.NewSource(int64(seed)))
	data := make(map[string]string)
	for j := 0; j < 5; j++ {
		key := fmt.Sprintf("key-%d-%d", j, r.Int())
		value := fmt.Sprintf("value-%d-%d", seed, r.Int())
		data[key] = value
	}
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      fmt.Sprintf("cm-%d-%d", seed, r.Int()),
			Labels: map[string]string{
				"bench": "crci",
				"seed":  fmt.Sprintf("%d", seed),
				"r":     fmt.Sprintf("%d", r.Int()),
			},
		},
		Data: data,
	}
}

// fuzzSecret returns a Secret with deterministic but varied data.
func fuzzSecret(namespace string, seed int) *corev1.Secret {
	r := rand.New(rand.NewSource(int64(seed)))
	data := make(map[string][]byte)
	for j := 0; j < 5; j++ {
		key := fmt.Sprintf("key-%d-%d", j, r.Int())
		value := []byte(fmt.Sprintf("value-%d-%d", seed, r.Int()))
		data[key] = value
	}
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      fmt.Sprintf("secret-%d-%d", seed, r.Int()),
			Labels: map[string]string{
				"bench": "crci",
				"seed":  fmt.Sprintf("%d", seed),
			},
		},
		Data: data,
		Type: corev1.SecretTypeOpaque,
	}
}

// fuzzPod returns a minimal valid Pod with fuzz-style metadata.
func fuzzPod(namespace string, seed int) *corev1.Pod {
	r := rand.New(rand.NewSource(int64(seed)))
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      fmt.Sprintf("pod-%d-%d", seed, r.Int()),
			Labels: map[string]string{
				"bench": "crci",
				"seed":  fmt.Sprintf("%d", seed),
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{Name: "c", Image: "busybox", Command: []string{"sleep", "3600"}},
			},
			RestartPolicy: corev1.RestartPolicyNever,
		},
	}
}

func openAPISchemaType(s *openapispec.Schema) string {
	if s == nil || len(s.Type) == 0 {
		return ""
	}
	return s.Type[0]
}

func openAPIExtBool(s *openapispec.Schema, key string) bool {
	if s == nil || s.Extensions == nil {
		return false
	}
	b, _ := s.Extensions[key].(bool)
	return b
}

func openAPIExtString(s *openapispec.Schema, key string) string {
	if s == nil || s.Extensions == nil {
		return ""
	}
	str, _ := s.Extensions[key].(string)
	return str
}

func openAPIExtStrings(s *openapispec.Schema, key string) []string {
	if s == nil || s.Extensions == nil {
		return nil
	}
	raw, ok := s.Extensions[key].([]interface{})
	if !ok {
		return nil
	}
	var out []string
	for _, v := range raw {
		if str, ok := v.(string); ok {
			out = append(out, str)
		}
	}
	return out
}

const maxFuzzDepth = 8 // limit recursion for nested objects/lists

// quantitySuffix matches valid Kubernetes quantity suffix (e.g. 1Gi, 100Mi, 1).
var quantitySuffix = regexp.MustCompile(`^[0-9]+(\.[0-9]+)?([KMGTPE]i?|[eE][+-]?[0-9]+)?$`)

// quantityFull matches full Kubernetes quantity pattern (optional sign, number, optional suffix).
var quantityFull = regexp.MustCompile(`^(\+|-)?(([0-9]+(\.[0-9]*)?)|(\.[0-9]+))(([KMGTPE]i)|[numkMGTPE]|([eE](\+|-)?(([0-9]+(\.[0-9]*)?)|(\.[0-9]+))))?$`)

// reasonPattern matches condition reason: CamelCase / [A-Za-z][A-Za-z0-9_,:]*[A-Za-z0-9_]
var reasonPattern = regexp.MustCompile(`^[A-Za-z]([A-Za-z0-9_,:]*[A-Za-z0-9_])?$`)

// quantityLikeKeys are property names that must be valid Kubernetes quantity strings.
var quantityLikeKeys = map[string]bool{
	"size": true, "capacity": true, "bootDiskCapacity": true, "reservation": true,
	"limit": true, "requested": true, "used": true, "total": true, "other": true, "disks": true,
	"vm": true, "volume": true, // e.g. status.storage.usage.snapshots.{vm,volume}
}

// sanitizeFuzzedStatus recurses into status map and fixes invalid enum/quantity/reason values so validation passes.
func sanitizeFuzzedStatus(m map[string]interface{}) {
	for k, v := range m {
		switch vv := v.(type) {
		case map[string]interface{}:
			sanitizeFuzzedStatus(vv)
		case []interface{}:
			for _, el := range vv {
				if mm, ok := el.(map[string]interface{}); ok {
					sanitizeFuzzedStatus(mm)
				}
			}
		case string:
			keyLower := strings.ToLower(k)
			if quantityLikeKeys[keyLower] {
				if !quantityFull.MatchString(vv) {
					m[k] = "1Gi"
				}
				continue
			}
			if k == "reason" && !reasonPattern.MatchString(vv) {
				m[k] = "Ready"
				continue
			}
			// Fields that must be integer (e.g. unitNumber) but got a string (e.g. "string" from enum)
			if keyLower == "unitnumber" || strings.HasSuffix(keyLower, "unitnumber") {
				m[k] = float64(0)
				continue
			}
		case float64:
			// unitNumber must be integer; keep as float64 so DeepCopyJSONValue works (no Go int in nested maps)
			keyLower := strings.ToLower(k)
			if keyLower == "unitnumber" || strings.HasSuffix(keyLower, "unitnumber") {
				m[k] = math.Trunc(vv)
				continue
			}
		}
	}
}

// dnsLabel returns a DNS-compliant label (lowercase alphanumeric, hyphens allowed), at most maxLen chars.
func dnsLabel(maxLen int, seed int, r *rand.Rand) string {
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	if maxLen < 1 {
		maxLen = 1
	}
	n := 2 + r.Intn(maxLen-1) // at least 2 chars
	b := make([]byte, n)
	b[0] = chars[r.Intn(26)] // must start with letter
	for i := 1; i < n-1; i++ {
		b[i] = chars[r.Intn(len(chars))]
	}
	b[n-1] = chars[r.Intn(36)] // end alphanumeric
	return string(b)
}

// shortString returns a concise string (alphanumeric + spaces) at most maxLen chars, for message/error fields.
func shortString(maxLen int, seed int, r *rand.Rand) string {
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789 "
	if maxLen < 1 {
		maxLen = 1
	}
	n := 1 + r.Intn(maxLen)
	b := make([]byte, n)
	for i := 0; i < n; i++ {
		b[i] = chars[r.Intn(len(chars))]
	}
	return string(b)
}

// randReader implements io.Reader by reading random bytes from *rand.Rand (for deterministic UUID v4).
type randReader struct{ r *rand.Rand }

func (r *randReader) Read(p []byte) (n int, err error) {
	for i := range p {
		p[i] = byte(r.r.Intn(256))
	}
	return len(p), nil
}

// uuidV4 returns a UUID version 4 string using github.com/google/uuid (deterministic when r is seeded).
// Used for all UUID-valued fields (e.g. diskUUID, or any schema format "uuid").
func uuidV4(seed int, r *rand.Rand) string {
	u, err := uuid.NewRandomFromReader(&randReader{r: r})
	if err != nil {
		return uuid.New().String() // fallback to non-deterministic
	}
	return u.String()
}

// uniqueIDVM returns a string matching "vm-\d+" with 1–5 digits (e.g. status.uniqueID).
func uniqueIDVM(seed int, r *rand.Rand) string {
	digits := 1 + r.Intn(5)
	b := make([]byte, 3+digits)
	b[0], b[1], b[2] = 'v', 'm', '-'
	for i := 0; i < digits; i++ {
		b[3+i] = byte('0' + r.Intn(10))
	}
	return string(b)
}

// defaultForRequiredField returns a validation-friendly default for common required field names.
func defaultForRequiredField(name string, seed int, r *rand.Rand) interface{} {
	switch strings.ToLower(name) {
	case "size", "bootdiskcapacity", "capacity":
		return "1Gi"
	case "storageclass":
		return "default"
	case "kind":
		return "CronTab"
	case "apiversion":
		return "stable.example.com/v1"
	case "port":
		return float64(22)
	case "name":
		return dnsLabel(19, seed, r)
	case "message":
		return shortString(127, seed, r)
	case "error":
		return shortString(127, seed, r)
	case "zone", "keyid", "providerid":
		return shortString(63, seed, r)
	case "diskuuid":
		return uuidV4(seed, r)
	case "uniqueid":
		return uniqueIDVM(seed, r)
	case "automode":
		return "perSeat"
	case "reason":
		return "Ready"
	default:
		if strings.Contains(strings.ToLower(name), "uuid") {
			return uuidV4(seed, r)
		}
		return dnsLabel(32, seed, r)
	}
}

// enumFromAllOfOneOfAnyOfOpenAPI returns a merged enum from allOf (intersection), or first non-empty from oneOf/anyOf.
func enumFromAllOfOneOfAnyOfOpenAPI(s *openapispec.Schema) []interface{} {
	if s == nil {
		return nil
	}
	if len(s.AllOf) > 0 {
		var merged []string
		for i, sub := range s.AllOf {
			if len(sub.Enum) == 0 {
				continue
			}
			var vals []string
			for _, e := range sub.Enum {
				vals = append(vals, fmt.Sprintf("%v", e))
			}
			if i == 0 {
				merged = vals
			} else {
				set := make(map[string]bool)
				for _, v := range merged {
					set[v] = true
				}
				var next []string
				for _, v := range vals {
					if set[v] {
						next = append(next, v)
					}
				}
				merged = next
			}
		}
		if len(merged) > 0 {
			out := make([]interface{}, len(merged))
			for i, v := range merged {
				out[i] = v
			}
			return out
		}
	}
	for i := range s.OneOf {
		if len(s.OneOf[i].Enum) > 0 {
			return s.OneOf[i].Enum
		}
	}
	for i := range s.AnyOf {
		if len(s.AnyOf[i].Enum) > 0 {
			return s.AnyOf[i].Enum
		}
	}
	return nil
}

func valueFromDefaultOrExampleOpenAPI(s *openapispec.Schema) interface{} {
	if s == nil {
		return nil
	}
	if s.Default != nil {
		return s.Default
	}
	if s.Example != nil {
		return s.Example
	}
	return nil
}

// stringFromFormat returns a string valid for the OpenAPI format (e.g. date-time, uri, hostname).
func stringFromFormat(format string, seed int, r *rand.Rand) string {
	switch format {
	case "date":
		return time.Now().AddDate(0, 0, r.Intn(365)-182).Format("2006-01-02")
	case "date-time", "datetime":
		return time.Now().Add(time.Duration(r.Int63n(86400)) * time.Second).UTC().Format(time.RFC3339)
	case "duration":
		durs := []string{"0s", "1ns", "100ms", "1s", "60s", "1m", "1h"}
		return durs[r.Intn(len(durs))]
	case "uri":
		return fmt.Sprintf("https://example-%d.com/path/%d", seed, r.Int())
	case "hostname":
		return fmt.Sprintf("host-%d-%d.example.com", seed, r.Int())
	case "email":
		return fmt.Sprintf("user-%d-%d@example.com", seed, r.Int())
	case "ipv4":
		return fmt.Sprintf("10.%d.%d.%d", r.Intn(256), r.Intn(256), r.Intn(256))
	case "ipv6":
		// Simple valid IPv6
		return "2001:db8::1"
	case "cidr":
		return "10.0.0.0/24"
	case "mac":
		b := make([]byte, 6)
		for i := range b {
			b[i] = byte(r.Intn(256))
		}
		return net.HardwareAddr(b).String()
	case "uuid", "uuid3", "uuid4", "uuid5":
		return uuidV4(seed, r)
	case "byte":
		return base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("fuzz-%d", seed)))
	case "password":
		return fmt.Sprintf("fuzz-%d-%d", seed, r.Int())
	default:
		return ""
	}
}

// stringMatchingPatternOpenAPI returns a string that satisfies length constraints and, if possible, the pattern.
func stringMatchingPatternOpenAPI(s *openapispec.Schema, fieldName string, seed int, r *rand.Rand) string {
	if s == nil {
		return dnsLabel(32, seed, r)
	}
	minL, maxL := int64(0), int64(32)
	if s.MinLength != nil && *s.MinLength > 0 {
		minL = *s.MinLength
	}
	if s.MaxLength != nil && *s.MaxLength > 0 {
		maxL = *s.MaxLength
	}
	fieldLower := strings.ToLower(fieldName)
	switch fieldLower {
	case "name":
		if maxL > 19 {
			maxL = 19
		}
	case "message", "error":
		if maxL > 127 {
			maxL = 127
		}
	case "zone", "keyid", "providerid":
		if maxL > 63 {
			maxL = 63
		}
	case "diskuuid":
		return uuidV4(seed, r)
	case "uniqueid":
		return uniqueIDVM(seed, r)
	}
	if strings.Contains(fieldLower, "uuid") {
		return uuidV4(seed, r)
	}
	if maxL < minL {
		maxL = minL
	}
	if s.Format != "" {
		str := stringFromFormat(s.Format, seed, r)
		if str != "" {
			if int64(len(str)) > maxL {
				str = str[:maxL]
			}
			if int64(len(str)) < minL {
				str = str + strings.Repeat("x", int(minL)-len(str))
			}
			return str
		}
	}
	if s.Pattern != "" && (strings.Contains(s.Pattern, "KMGTPE") || strings.Contains(s.Pattern, "quantity") || strings.Contains(s.Pattern, "numkMGTPE")) {
		return "1Gi"
	}
	if s.Pattern != "" && strings.Contains(s.Pattern, "0-9A-Fa-f") && strings.Contains(s.Pattern, "){5}") {
		str := stringFromFormat("mac", seed, r)
		if int64(len(str)) > maxL {
			str = str[:maxL]
		}
		return str
	}
	if s.Pattern != "" && strings.Contains(s.Pattern, "[A-Za-z0-9_])?$") {
		reasons := []string{"Ready", "Pending", "Error", "FuzzReason", "Created", "Updated"}
		return reasons[r.Intn(len(reasons))]
	}
	if s.Pattern != "" && (strings.Contains(s.Pattern, `\w`) || strings.Contains(s.Pattern, "w+")) {
		const word = "abcdefghijklmnopqrstuvwxyz0123456789_"
		n := minL
		if n < 2 {
			n = 2
		}
		if maxL > minL && n < maxL {
			n = n + r.Int63n(maxL-n+1)
		}
		b := make([]byte, n)
		for i := int64(0); i < n; i++ {
			b[i] = word[r.Intn(len(word))]
		}
		return string(b)
	}
	return dnsLabel(int(maxL), seed, r)
}

// fuzzFromOpenAPISchema generates a fuzz value from an OpenAPI spec.Schema,
// resolving $ref from components when present.
func fuzzFromOpenAPISchema(s *openapispec.Schema, components map[string]*openapispec.Schema, visited map[string]bool, seed int, depth int, fieldName string) interface{} {
	if depth > maxFuzzDepth {
		return nil
	}
	r := rand.New(rand.NewSource(int64(seed)))
	if s == nil {
		return nil
	}
	// Resolve $ref so we fuzz the actual schema
	if s.Ref.String() != "" {
		resolved := ResolveRef(s, components, visited)
		if resolved != nil {
			s = resolved
		}
	}
	if v := valueFromDefaultOrExampleOpenAPI(s); v != nil {
		return v
	}
	enum := s.Enum
	if len(enum) == 0 {
		enum = enumFromAllOfOneOfAnyOfOpenAPI(s)
	}
	if len(enum) > 0 {
		v := enum[r.Intn(len(enum))]
		if str, ok := v.(string); ok && (str == "string" || str == "integer") {
			fieldLower := strings.ToLower(fieldName)
			if strings.Contains(fieldLower, "unitnumber") {
				return float64(r.Intn(16))
			}
		}
		return v
	}
	if openAPIExtBool(s, "x-kubernetes-int-or-string") {
		if r.Intn(2) == 0 {
			return float64(r.Intn(1000))
		}
		return dnsLabel(19, seed, r)
	}
	typ := openAPISchemaType(s)
	switch typ {
	case "string":
		str := stringMatchingPatternOpenAPI(s, fieldName, seed, r)
		if str == "" {
			switch strings.ToLower(fieldName) {
			case "name":
				str = dnsLabel(19, seed, r)
			case "message", "error":
				str = shortString(127, seed, r)
			case "zone", "keyid", "providerid":
				str = shortString(63, seed, r)
			case "diskuuid":
				str = uuidV4(seed, r)
			case "uniqueid":
				str = uniqueIDVM(seed, r)
			default:
				if strings.Contains(strings.ToLower(fieldName), "uuid") {
					str = uuidV4(seed, r)
				} else {
					str = dnsLabel(32, seed, r)
				}
			}
		}
		return str
	case "boolean":
		return r.Intn(2) == 1
	case "integer", "number":
		val := float64(r.Intn(10000))
		if s.Minimum != nil && val < *s.Minimum {
			val = *s.Minimum
		}
		if s.Maximum != nil && val > *s.Maximum {
			val = *s.Maximum
		}
		if s.ExclusiveMinimum && s.Minimum != nil {
			val = math.Max(val, *s.Minimum+1)
		}
		if s.ExclusiveMaximum && s.Maximum != nil {
			val = math.Min(val, *s.Maximum-1)
		}
		if s.MultipleOf != nil && *s.MultipleOf > 0 {
			val = math.Floor(val/(*s.MultipleOf)) * (*s.MultipleOf)
		}
		return val
	case "array":
		var itemSchema *openapispec.Schema
		if s.Items != nil {
			itemSchema = s.Items.Schema
			if itemSchema == nil && len(s.Items.Schemas) > 0 {
				itemSchema = &s.Items.Schemas[0]
			}
		}
		minItems, maxItems := int64(0), int64(3)
		if s.MinItems != nil {
			minItems = *s.MinItems
		}
		if s.MaxItems != nil && *s.MaxItems > 0 {
			maxItems = *s.MaxItems
		}
		if maxItems < minItems {
			maxItems = minItems
		}
		n := minItems
		if maxItems > minItems {
			n = minItems + r.Int63n(maxItems-minItems+1)
		}
		if itemSchema != nil && n == 0 {
			n = 1
		}
		arr := make([]interface{}, 0, n)
		listType := openAPIExtString(s, "x-kubernetes-list-type")
		listMapKeys := openAPIExtStrings(s, "x-kubernetes-list-map-keys")
		seen := make(map[string]bool)
		for i := int64(0); i < n; i++ {
			if itemSchema == nil {
				arr = append(arr, dnsLabel(32, seed+int(i), r))
				continue
			}
			if depth+1 > maxFuzzDepth {
				it := openAPISchemaType(itemSchema)
				if it == "object" || len(itemSchema.Properties) > 0 {
					placeholder := map[string]interface{}{}
					arr = append(arr, placeholder)
					// Set list-map keys on placeholder so entries remain unique (no duplicate key values).
					if listType == "map" && len(listMapKeys) > 0 {
						for _, listKey := range listMapKeys {
							keyLower := strings.ToLower(listKey)
							if keyLower == "busnumber" || keyLower == "unitnumber" {
								placeholder[listKey] = float64(i)
							} else if propSchema, ok := itemSchema.Properties[listKey]; ok && (openAPISchemaType(&propSchema) == "integer" || openAPISchemaType(&propSchema) == "number") {
								placeholder[listKey] = float64(i)
							} else {
								placeholder[listKey] = fmt.Sprintf("key-%d-%d", seed+int(i), r.Int())
							}
						}
					}
				} else if it == "array" {
					arr = append(arr, []interface{}{})
				} else {
					arr = append(arr, dnsLabel(32, seed+int(i), r))
				}
				continue
			}
			item := fuzzFromOpenAPISchema(itemSchema, components, copyVisitedOpenAPI(visited), seed+int(i)*100, depth+1, "")
			if item == nil {
				it := openAPISchemaType(itemSchema)
				if it == "object" || len(itemSchema.Properties) > 0 {
					item = map[string]interface{}{}
				} else if it == "array" {
					item = []interface{}{}
				} else {
					item = dnsLabel(32, seed+int(i), r)
				}
			}
			// x-kubernetes-list-type=map with x-kubernetes-list-map-keys: each key must be
			// unique per entry (see https://kubernetes.io/docs/reference/using-api/server-side-apply/).
			if listType == "map" && len(listMapKeys) > 0 {
				if m, ok := item.(map[string]interface{}); ok {
					for _, listKey := range listMapKeys {
						keyLower := strings.ToLower(listKey)
						switch keyLower {
						case "busnumber", "unitnumber":
							// Numeric list-map keys: use index so each entry is unique.
							m[listKey] = float64(i)
						case "name":
							m[listKey] = dnsLabel(19, seed+int(i), r)
						case "type":
							if propSchema, ok := itemSchema.Properties[listKey]; ok {
								penum := propSchema.Enum
								if len(penum) == 0 {
									penum = enumFromAllOfOneOfAnyOfOpenAPI(&propSchema)
								}
								if len(penum) > 0 {
									m[listKey] = penum[r.Intn(len(penum))]
								} else {
									m[listKey] = fmt.Sprintf("key-%d-%d", seed+int(i), r.Int())
								}
							} else {
								m[listKey] = fmt.Sprintf("key-%d-%d", seed+int(i), r.Int())
							}
						default:
							// Use schema type for list-map key: integer/number need unique numeric values.
							if propSchema, ok := itemSchema.Properties[listKey]; ok {
								pt := openAPISchemaType(&propSchema)
								if pt == "integer" || pt == "number" {
									m[listKey] = float64(i)
									break
								}
							}
							m[listKey] = fmt.Sprintf("key-%d-%d", seed+int(i), r.Int())
						}
					}
				}
			}
			if listType == "set" {
				key := fmt.Sprintf("%v", item)
				if seen[key] {
					continue
				}
				seen[key] = true
			}
			arr = append(arr, item)
		}
		return arr
	case "object":
		out := make(map[string]interface{})
		if openAPIExtBool(s, "x-kubernetes-embedded-resource") {
			out["apiVersion"] = "v1"
			out["kind"] = "Generic"
			out["metadata"] = map[string]interface{}{"name": dnsLabel(19, seed, r)}
		}
		requiredSet := make(map[string]bool)
		for _, req := range s.Required {
			requiredSet[req] = true
		}
		for propName, propSchema := range s.Properties {
			if propName == "metadata" || propName == "apiVersion" || propName == "kind" {
				if !openAPIExtBool(s, "x-kubernetes-embedded-resource") {
					continue
				}
			}
			prop := propSchema
			v := fuzzFromOpenAPISchema(&prop, components, copyVisitedOpenAPI(visited), seed+int(r.Int63n(100)), depth+1, propName)
			if v == nil {
				if requiredSet[propName] {
					v = defaultForRequiredField(propName, seed, r)
				} else if openAPISchemaType(&propSchema) == "object" || len(propSchema.Properties) > 0 {
					v = map[string]interface{}{}
				} else if openAPISchemaType(&propSchema) == "array" {
					v = []interface{}{}
				}
			}
			if v != nil {
				out[propName] = v
			}
		}
		for req := range requiredSet {
			if _, ok := out[req]; ok {
				continue
			}
			if req == "metadata" || req == "apiVersion" || req == "kind" {
				continue
			}
			out[req] = defaultForRequiredField(req, seed, r)
		}
		for _, key := range []string{"size", "storageClass", "kind", "apiVersion", "bootDiskCapacity", "capacity"} {
			if requiredSet[key] {
				out[key] = defaultForRequiredField(key, seed, r)
			}
		}
		for key := range quantityLikeKeys {
			if v, ok := out[key]; ok {
				if str, ok := v.(string); ok && !quantityFull.MatchString(str) {
					out[key] = "1Gi"
				}
			}
		}
		return out
	}
	if len(s.Properties) > 0 {
		return fuzzFromOpenAPISchema(s, components, visited, seed, depth, "")
	}
	for i := range s.OneOf {
		sub := &s.OneOf[i]
		if openAPISchemaType(sub) == "object" && len(sub.Properties) > 0 {
			return fuzzFromOpenAPISchema(sub, components, copyVisitedOpenAPI(visited), seed, depth, fieldName)
		}
		if len(sub.Properties) > 0 {
			return fuzzFromOpenAPISchema(sub, components, copyVisitedOpenAPI(visited), seed, depth, fieldName)
		}
		if openAPISchemaType(sub) == "array" && sub.Items != nil {
			return fuzzFromOpenAPISchema(sub, components, copyVisitedOpenAPI(visited), seed, depth, fieldName)
		}
		if openAPISchemaType(sub) == "integer" || openAPISchemaType(sub) == "number" {
			return fuzzFromOpenAPISchema(sub, components, copyVisitedOpenAPI(visited), seed, depth, fieldName)
		}
	}
	for i := range s.AnyOf {
		sub := &s.AnyOf[i]
		if openAPISchemaType(sub) == "object" && len(sub.Properties) > 0 {
			return fuzzFromOpenAPISchema(sub, components, copyVisitedOpenAPI(visited), seed, depth, fieldName)
		}
		if len(sub.Properties) > 0 {
			return fuzzFromOpenAPISchema(sub, components, copyVisitedOpenAPI(visited), seed, depth, fieldName)
		}
		if openAPISchemaType(sub) == "array" && sub.Items != nil {
			return fuzzFromOpenAPISchema(sub, components, copyVisitedOpenAPI(visited), seed, depth, fieldName)
		}
		if openAPISchemaType(sub) == "integer" || openAPISchemaType(sub) == "number" {
			return fuzzFromOpenAPISchema(sub, components, copyVisitedOpenAPI(visited), seed, depth, fieldName)
		}
	}
	return nil
}

func copyVisitedOpenAPI(m map[string]bool) map[string]bool {
	if m == nil {
		return nil
	}
	c := make(map[string]bool, len(m))
	for k, v := range m {
		c[k] = v
	}
	return c
}

// fuzzUnstructured returns an Unstructured for the given GVK, using the
// OpenAPI root schema and components when available to guide fuzz.
func fuzzUnstructured(
	rootSchema *openapispec.Schema,
	components map[string]*openapispec.Schema,
	gvk schema.GroupVersionKind,
	namespace string,
	seed int) *unstructured.Unstructured {

	r := rand.New(rand.NewSource(int64(seed)))
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(gvk)
	u.SetNamespace(namespace)
	u.SetName(fmt.Sprintf("%s-%d-%d", strings.ToLower(gvk.Kind), seed, r.Int()))
	u.SetLabels(map[string]string{"bench": "crci", "seed": fmt.Sprintf("%d", seed)})

	if rootSchema != nil && components != nil {
		if specSchema, ok := rootSchema.Properties["spec"]; ok {
			spec := specSchema
			if specVal := fuzzFromOpenAPISchema(&spec, components, nil, seed, 0, ""); specVal != nil {
				if m, ok := specVal.(map[string]interface{}); ok {
					_ = unstructured.SetNestedField(u.Object, m, "spec")
				}
			}
		}
		if statusSchema, ok := rootSchema.Properties["status"]; ok {
			status := statusSchema
			if statusVal := fuzzFromOpenAPISchema(&status, components, nil, seed+1000, 0, ""); statusVal != nil {
				if m, ok := statusVal.(map[string]interface{}); ok {
					sanitizeFuzzedStatus(m)
					_ = unstructured.SetNestedField(u.Object, m, "status")
				}
			}
		}
	}
	if _, hasSpec := u.Object["spec"]; !hasSpec {
		_ = unstructured.SetNestedField(u.Object, map[string]interface{}{}, "spec")
	}

	return u
}
