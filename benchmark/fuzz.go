package benchmark

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"net"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// fuzzConfigMap returns a ConfigMap with deterministic but varied data to simulate fuzz-loaded resources.
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

// crdSchemaForGVK returns the OpenAPIV3Schema for the given GVK from the CRD
// list, or nil.
func crdSchemaForGVK(
	crds []*apiextensionsv1.CustomResourceDefinition,
	gvk schema.GroupVersionKind) *apiextensionsv1.JSONSchemaProps {

	for _, crd := range crds {
		if crd.Spec.Group != gvk.Group {
			continue
		}
		if crd.Spec.Names.Kind != gvk.Kind {
			continue
		}
		for _, v := range crd.Spec.Versions {
			if v.Name != gvk.Version {
				continue
			}
			if v.Schema != nil && v.Schema.OpenAPIV3Schema != nil {
				return v.Schema.OpenAPIV3Schema
			}
			return nil
		}
	}
	return nil
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

// enumFromAllOfOneOfAnyOf returns a merged enum from allOf (intersection), or first non-empty from oneOf/anyOf.
func enumFromAllOfOneOfAnyOf(schema *apiextensionsv1.JSONSchemaProps) []apiextensionsv1.JSON {
	if schema == nil {
		return nil
	}
	if len(schema.AllOf) > 0 {
		var merged []string
		for i, sub := range schema.AllOf {
			if len(sub.Enum) == 0 {
				continue
			}
			var vals []string
			for _, e := range sub.Enum {
				var v interface{}
				if err := json.Unmarshal(e.Raw, &v); err == nil {
					vals = append(vals, fmt.Sprintf("%v", v))
				}
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
			out := make([]apiextensionsv1.JSON, len(merged))
			for i, s := range merged {
				raw, _ := json.Marshal(s)
				out[i] = apiextensionsv1.JSON{Raw: raw}
			}
			return out
		}
	}
	for _, sub := range schema.OneOf {
		if len(sub.Enum) > 0 {
			return sub.Enum
		}
	}
	for _, sub := range schema.AnyOf {
		if len(sub.Enum) > 0 {
			return sub.Enum
		}
	}
	return nil
}

// valueFromDefaultOrExample returns a value from schema.Default or schema.Example if set.
func valueFromDefaultOrExample(schema *apiextensionsv1.JSONSchemaProps) interface{} {
	if schema.Default != nil && len(schema.Default.Raw) > 0 {
		var v interface{}
		if err := json.Unmarshal(schema.Default.Raw, &v); err == nil {
			return v
		}
	}
	if schema.Example != nil && len(schema.Example.Raw) > 0 {
		var v interface{}
		if err := json.Unmarshal(schema.Example.Raw, &v); err == nil {
			return v
		}
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

// stringMatchingPattern returns a string that satisfies length constraints and, if possible, the pattern.
// Default: DNS-compliant, ≤32 chars, unless schema or field-specific rules say otherwise.
func stringMatchingPattern(schema *apiextensionsv1.JSONSchemaProps, fieldName string, seed int, r *rand.Rand) string {
	minL, maxL := int64(0), int64(32) // default: DNS-compliant ≤32 unless schema/field constrains
	if schema.MinLength != nil && *schema.MinLength > 0 {
		minL = *schema.MinLength
	}
	if schema.MaxLength != nil && *schema.MaxLength > 0 {
		maxL = *schema.MaxLength
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
	// Any field name containing "uuid" is treated as UUID v4 (e.g. biosUUID, instanceUUID).
	if strings.Contains(fieldLower, "uuid") {
		return uuidV4(seed, r)
	}
	if maxL < minL {
		maxL = minL
	}
	// If Format is set, use it (and trim to maxLength)
	if schema.Format != "" {
		s := stringFromFormat(schema.Format, seed, r)
		if s != "" {
			if int64(len(s)) > maxL {
				s = s[:maxL]
			}
			if int64(len(s)) < minL {
				s = s + strings.Repeat("x", int(minL)-len(s))
			}
			return s
		}
	}
	// Kubernetes quantity-like pattern
	if schema.Pattern != "" && (strings.Contains(schema.Pattern, "KMGTPE") || strings.Contains(schema.Pattern, "quantity") || strings.Contains(schema.Pattern, "numkMGTPE")) {
		return "1Gi"
	}
	// MAC address pattern: ^([0-9A-Fa-f]{2}:){5}([0-9A-Fa-f]{2})$
	if schema.Pattern != "" && strings.Contains(schema.Pattern, "0-9A-Fa-f") && strings.Contains(schema.Pattern, "){5}") {
		s := stringFromFormat("mac", seed, r)
		if int64(len(s)) > maxL {
			s = s[:maxL]
		}
		return s
	}
	// Condition reason: ^[A-Za-z]([A-Za-z0-9_,:]*[A-Za-z0-9_])?$
	if schema.Pattern != "" && strings.Contains(schema.Pattern, "[A-Za-z0-9_])?$") {
		reasons := []string{"Ready", "Pending", "Error", "FuzzReason", "Created", "Updated"}
		return reasons[r.Intn(len(reasons))]
	}
	// Word chars only: \w+ (alphanumeric and underscore)
	if schema.Pattern != "" && (strings.Contains(schema.Pattern, `\w`) || strings.Contains(schema.Pattern, "w+")) {
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
	// Generic string: DNS-compliant label, constrained by schema or field rules above
	return dnsLabel(int(maxL), seed, r)
}

// fuzzFromSchema generates a fuzz value from a JSONSchemaProps using type, Format, Pattern,
// Enum, Default, Example, Min/Max length, Min/Max/MultipleOf, and x-kubernetes-* extensions.
// fieldName is the property name when recursing from an object (for name/message/error caps); use "" otherwise.
func fuzzFromSchema(schema *apiextensionsv1.JSONSchemaProps, seed int, depth int, fieldName string) interface{} {
	if depth > maxFuzzDepth {
		return nil
	}
	r := rand.New(rand.NewSource(int64(seed)))
	if schema == nil {
		return nil
	}
	// Default or Example take precedence when present
	if v := valueFromDefaultOrExample(schema); v != nil {
		return v
	}
	// Enum: explicit set of valid values (including from allOf/oneOf/anyOf)
	enum := schema.Enum
	if len(enum) == 0 {
		enum = enumFromAllOfOneOfAnyOf(schema)
	}
	if len(enum) > 0 {
		raw := enum[r.Intn(len(enum))].Raw
		var v interface{}
		if err := json.Unmarshal(raw, &v); err == nil {
			// Don't use enum value "string" or "integer" for fields that must be integer (e.g. unitNumber)
			if s, ok := v.(string); ok && (s == "string" || s == "integer") {
				fieldLower := strings.ToLower(fieldName)
				if strings.Contains(fieldLower, "unitnumber") {
					return float64(r.Intn(16))
				}
			}
			return v
		}
	}
	// x-kubernetes-int-or-string: value may be int or string
	if schema.XIntOrString {
		if r.Intn(2) == 0 {
			return float64(r.Intn(1000))
		}
		return dnsLabel(19, seed, r)
	}
	switch schema.Type {
	case "string":
		s := stringMatchingPattern(schema, fieldName, seed, r)
		if s == "" {
			switch strings.ToLower(fieldName) {
			case "name":
				s = dnsLabel(19, seed, r)
			case "message", "error":
				s = shortString(127, seed, r)
			case "zone", "keyid", "providerid":
				s = shortString(63, seed, r)
			case "diskuuid":
				s = uuidV4(seed, r)
			case "uniqueid":
				s = uniqueIDVM(seed, r)
			default:
				if strings.Contains(strings.ToLower(fieldName), "uuid") {
					s = uuidV4(seed, r)
				} else {
					s = dnsLabel(32, seed, r)
				}
			}
		}
		return s
	case "boolean":
		return r.Intn(2) == 1
	case "integer", "number":
		val := float64(r.Intn(10000))
		if schema.Minimum != nil && val < *schema.Minimum {
			val = *schema.Minimum
		}
		if schema.Maximum != nil && val > *schema.Maximum {
			val = *schema.Maximum
		}
		if schema.ExclusiveMinimum && schema.Minimum != nil {
			val = math.Max(val, *schema.Minimum+1)
		}
		if schema.ExclusiveMaximum && schema.Maximum != nil {
			val = math.Min(val, *schema.Maximum-1)
		}
		if schema.MultipleOf != nil && *schema.MultipleOf > 0 {
			val = math.Floor(val/(*schema.MultipleOf)) * (*schema.MultipleOf)
		}
		return val
	case "array":
		itemSchema := (*apiextensionsv1.JSONSchemaProps)(nil)
		if schema.Items != nil {
			itemSchema = schema.Items.Schema
			if itemSchema == nil && len(schema.Items.JSONSchemas) > 0 {
				itemSchema = &schema.Items.JSONSchemas[0]
			}
		}
		minItems, maxItems := int64(0), int64(3)
		if schema.MinItems != nil {
			minItems = *schema.MinItems
		}
		if schema.MaxItems != nil && *schema.MaxItems > 0 {
			maxItems = *schema.MaxItems
		}
		if maxItems < minItems {
			maxItems = minItems
		}
		n := minItems
		if maxItems > minItems {
			n = minItems + r.Int63n(maxItems-minItems+1)
		}
		// When we have an item schema, generate at least one item so the list is not empty
		if itemSchema != nil && n == 0 {
			n = 1
		}
		arr := make([]interface{}, 0, n)
		listType := ""
		if schema.XListType != nil {
			listType = *schema.XListType
		}
		listMapKeys := schema.XListMapKeys
		seen := make(map[string]bool) // for list-type "set" uniqueness
		for i := int64(0); i < n; i++ {
			if itemSchema == nil {
				arr = append(arr, dnsLabel(32, seed+int(i), r))
				continue
			}
			if depth+1 > maxFuzzDepth {
				// At depth limit: still add a placeholder object or array so the list is non-empty
				if itemSchema.Type == "object" || len(itemSchema.Properties) > 0 {
					arr = append(arr, map[string]interface{}{})
				} else if itemSchema.Type == "array" {
					arr = append(arr, []interface{}{})
				} else {
					arr = append(arr, dnsLabel(32, seed+int(i), r))
				}
				continue
			}
			item := fuzzFromSchema(itemSchema, seed+int(i)*100, depth+1, "")
			if item == nil {
				if itemSchema.Type == "object" || len(itemSchema.Properties) > 0 {
					item = map[string]interface{}{}
				} else if itemSchema.Type == "array" {
					item = []interface{}{}
				} else {
					item = dnsLabel(32, seed+int(i), r)
				}
			}
			// list-type "map": each item must have the map keys set (required for identification).
			// Use schema enum for "type" so we get valid values (e.g. IDE/NVME/SCSI/SATA, Classic/Managed).
			if listType == "map" && len(listMapKeys) > 0 {
				if m, ok := item.(map[string]interface{}); ok {
					for _, listKey := range listMapKeys {
						keyLower := strings.ToLower(listKey)
						switch keyLower {
						case "busnumber":
							m[listKey] = float64(i)
						case "name":
							m[listKey] = dnsLabel(19, seed+int(i), r)
						case "type":
							if propSchema, ok := itemSchema.Properties[listKey]; ok {
								enum := propSchema.Enum
								if len(enum) == 0 {
									enum = enumFromAllOfOneOfAnyOf(&propSchema)
								}
								if len(enum) > 0 {
									raw := enum[r.Intn(len(enum))].Raw
									var v interface{}
									if err := json.Unmarshal(raw, &v); err == nil {
										m[listKey] = v
									} else {
										m[listKey] = fmt.Sprintf("key-%d-%d", seed+int(i), r.Int())
									}
								} else {
									m[listKey] = fmt.Sprintf("key-%d-%d", seed+int(i), r.Int())
								}
							} else {
								m[listKey] = fmt.Sprintf("key-%d-%d", seed+int(i), r.Int())
							}
						default:
							m[listKey] = fmt.Sprintf("key-%d-%d", seed+int(i), r.Int())
						}
					}
				}
			}
			// list-type "set": avoid duplicate scalar/object identity
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
		// x-kubernetes-embedded-resource: object must have apiVersion, kind, metadata
		if schema.XEmbeddedResource {
			out["apiVersion"] = "v1"
			out["kind"] = "Generic"
			out["metadata"] = map[string]interface{}{
				"name": dnsLabel(19, seed, r),
			}
		}
		requiredSet := make(map[string]bool)
		for _, req := range schema.Required {
			requiredSet[req] = true
		}
		for propName, propSchema := range schema.Properties {
			if propName == "metadata" || propName == "apiVersion" || propName == "kind" {
				if !schema.XEmbeddedResource {
					continue
				}
			}
			v := fuzzFromSchema(&propSchema, seed+int(r.Int63n(100)), depth+1, propName)
			if v == nil {
				if requiredSet[propName] {
					v = defaultForRequiredField(propName, seed, r)
				} else if propSchema.Type == "object" || len(propSchema.Properties) > 0 {
					v = map[string]interface{}{}
				} else if propSchema.Type == "array" {
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
		// Force valid quantity for any quantity-like field that got a non-quantity string
		for key := range quantityLikeKeys {
			if v, ok := out[key]; ok {
				if s, ok := v.(string); ok && !quantityFull.MatchString(s) {
					out[key] = "1Gi"
				}
			}
		}
		return out
	}
	if len(schema.Properties) > 0 {
		return fuzzFromSchema(&apiextensionsv1.JSONSchemaProps{Type: "object", Properties: schema.Properties, Required: schema.Required}, seed, depth, "")
	}
	// oneOf/anyOf: pick first subschema that is object, array, integer, or number and fuzz it
	for _, sub := range schema.OneOf {
		if sub.Type == "object" && len(sub.Properties) > 0 {
			return fuzzFromSchema(&sub, seed, depth, fieldName)
		}
		if len(sub.Properties) > 0 {
			return fuzzFromSchema(&apiextensionsv1.JSONSchemaProps{Type: "object", Properties: sub.Properties, Required: sub.Required}, seed, depth, fieldName)
		}
		if sub.Type == "array" && sub.Items != nil {
			return fuzzFromSchema(&sub, seed, depth, fieldName)
		}
		if sub.Type == "integer" || sub.Type == "number" {
			return fuzzFromSchema(&sub, seed, depth, fieldName)
		}
	}
	for _, sub := range schema.AnyOf {
		if sub.Type == "object" && len(sub.Properties) > 0 {
			return fuzzFromSchema(&sub, seed, depth, fieldName)
		}
		if len(sub.Properties) > 0 {
			return fuzzFromSchema(&apiextensionsv1.JSONSchemaProps{Type: "object", Properties: sub.Properties, Required: sub.Required}, seed, depth, fieldName)
		}
		if sub.Type == "array" && sub.Items != nil {
			return fuzzFromSchema(&sub, seed, depth, fieldName)
		}
		if sub.Type == "integer" || sub.Type == "number" {
			return fuzzFromSchema(&sub, seed, depth, fieldName)
		}
	}
	return nil
}

// fuzzUnstructured returns an Unstructured for the given GVK, using the scheme
// and CRD schema when available to guide fuzz.
func fuzzUnstructured(
	crds []*apiextensionsv1.CustomResourceDefinition,
	gvk schema.GroupVersionKind,
	namespace string,
	seed int) *unstructured.Unstructured {

	r := rand.New(rand.NewSource(int64(seed)))
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(gvk)
	u.SetNamespace(namespace)
	u.SetName(fmt.Sprintf("%s-%d-%d", strings.ToLower(gvk.Kind), seed, r.Int()))
	u.SetLabels(map[string]string{"bench": "crci", "seed": fmt.Sprintf("%d", seed)})

	rootSchema := crdSchemaForGVK(crds, gvk)
	if rootSchema != nil {
		// Root schema usually has properties: apiVersion, kind, metadata, spec,
		// status. Generate fuzz for spec (and optionally status).
		if specSchema, ok := rootSchema.Properties["spec"]; ok {
			if specVal := fuzzFromSchema(&specSchema, seed, 0, ""); specVal != nil {
				if m, ok := specVal.(map[string]interface{}); ok {
					_ = unstructured.SetNestedField(u.Object, m, "spec")
				}
			}
		}
		if statusSchema, ok := rootSchema.Properties["status"]; ok {
			if statusVal := fuzzFromSchema(&statusSchema, seed+1000, 0, ""); statusVal != nil {
				if m, ok := statusVal.(map[string]interface{}); ok {
					sanitizeFuzzedStatus(m)
					_ = unstructured.SetNestedField(u.Object, m, "status")
				}
			}
		}
	}
	// Ensure spec exists if we didn't set it from schema
	if _, hasSpec := u.Object["spec"]; !hasSpec {
		_ = unstructured.SetNestedField(u.Object, map[string]interface{}{}, "spec")
	}

	return u
}
