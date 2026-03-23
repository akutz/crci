package openapi

import (
	"encoding/base64"
	"fmt"
	"math"
	"math/rand"
	"net"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	openapi3 "k8s.io/client-go/openapi3"
	"k8s.io/client-go/rest"
	"k8s.io/kube-openapi/pkg/spec3"
	openapispec "k8s.io/kube-openapi/pkg/validation/spec"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/akutz/crci/benchmark/fuzzer"
)

// New returns a new fuzzer that fuzzes the given GVK using the API server's
// OpenAPI v3 schema.
func New(
	config *rest.Config,
	gvk schema.GroupVersionKind) fuzzer.FuzzerFn {

	// Schema for fuzzing is discovered from the API server's OpenAPI v3, not
	// from CRD files.
	var (
		rootSchema *openapispec.Schema
		components map[string]*openapispec.Schema
	)
	schemaResult, err := fetchSchemaForGVK(config, gvk)
	if err != nil {
		fmt.Fprintf(
			os.Stderr,
			" (openapi schema: %v, fuzzing with minimal spec) ",
			err)
	} else {
		rootSchema = schemaResult.RootSchema
		components = schemaResult.Components
	}

	return func(obj client.Object, namespace string, i int) error {
		switch tObj := obj.(type) {
		case *unstructured.Unstructured:
			fuzzUnstructured(gvk, rootSchema, components, tObj, namespace, i)
		default:
			return fmt.Errorf("unsupported object type: %T", obj)
		}
		return nil
	}
}

// fuzzUnstructured returns an Unstructured for the given GVK, using
// the OpenAPI root schema and components when available to guide fuzz.
func fuzzUnstructured(
	gvk schema.GroupVersionKind,
	rootSchema *openapispec.Schema,
	components map[string]*openapispec.Schema,
	obj *unstructured.Unstructured,
	namespace string,
	seed int) {

	r := rand.New(rand.NewSource(int64(seed)))
	obj.SetNamespace(namespace)
	obj.SetName(fmt.Sprintf("%s-%d-%d", strings.ToLower(gvk.Kind), seed, r.Int()))
	obj.SetLabels(map[string]string{"bench": "crci", "seed": fmt.Sprintf("%d", seed)})

	if rootSchema != nil && components != nil {
		if specSchema, ok := rootSchema.Properties["spec"]; ok {
			spec := specSchema
			if specVal := fuzzFromOpenAPISchema(&spec, components, nil, seed, 0, gvk, "spec"); specVal != nil {
				if m, ok := specVal.(map[string]interface{}); ok {
					_ = unstructured.SetNestedField(obj.Object, m, "spec")
				}
			}
		}
		if statusSchema, ok := rootSchema.Properties["status"]; ok {
			status := statusSchema
			if statusVal := fuzzFromOpenAPISchema(&status, components, nil, seed+1000, 0, gvk, "status"); statusVal != nil {
				if m, ok := statusVal.(map[string]interface{}); ok {
					sanitizeFuzzedStatus(m, &status, components)
					_ = unstructured.SetNestedField(obj.Object, m, "status")
				}
			}
		}
	}
}

// schemaResult holds the root schema for a GVK and the components map
// for resolving $ref when fuzzing.
type schemaResult struct {
	RootSchema *openapispec.Schema
	Components map[string]*openapispec.Schema
}

// fetchSchemaForGVK fetches the OpenAPI v3 schema for the given GVK from the
// API server. The CRD is still used to register the API (via envtest), but
// schema discovery for fuzzing comes from the server's published OpenAPI.
// See https://kubernetes.io/docs/concepts/overview/kubernetes-api/
func fetchSchemaForGVK(
	cfg *rest.Config,
	gvk schema.GroupVersionKind) (*schemaResult, error) {

	disc, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("discovery client: %w", err)
	}

	openapiClient := disc.OpenAPIV3()
	if openapiClient == nil {
		return nil, fmt.Errorf("OpenAPI v3 not supported")
	}

	root := openapi3.NewRoot(openapiClient)

	var (
		spec    *spec3.OpenAPI
		specErr error
		gv      = gvk.GroupVersion()
	)

	for i := range 5 {
		spec, specErr = root.GVSpec(gv)
		if specErr == nil && spec != nil {
			break
		}
		if i < 4 {
			time.Sleep(1 * time.Second)
		} else {
			return nil, fmt.Errorf(
				"fetch OpenAPI v3 for %s: %w", gv.String(), specErr)
		}
	}

	if spec == nil || spec.Components == nil || spec.Components.Schemas == nil {
		return nil, fmt.Errorf("no schemas in OpenAPI v3 for %s", gv.String())
	}
	rootSchema := findSchemaForGVK(spec.Components.Schemas, gvk)
	if rootSchema == nil {
		return nil, fmt.Errorf("no schema found for %s in OpenAPI v3", gvk.String())
	}
	// Resolve root schema $ref chain until we have concrete Properties (e.g. spec, status).
	// Built-in and CRD types often have a top-level schema that is only a $ref or allOf.
	visited := make(map[string]bool)
	for rootSchema.Ref.String() != "" {
		resolved := resolveRef(rootSchema, spec.Components.Schemas, visited)
		if resolved == nil {
			break
		}
		rootSchema = resolved
		if len(rootSchema.Properties) > 0 {
			break
		}
	}
	// Kubernetes OpenAPI v3 often uses allOf; take the first sub-schema that has Properties.
	if len(rootSchema.Properties) == 0 && len(rootSchema.AllOf) > 0 {
		for i := range rootSchema.AllOf {
			sub := &rootSchema.AllOf[i]
			resolved := sub
			if sub.Ref.String() != "" {
				resolved = resolveRef(sub, spec.Components.Schemas, nil)
				if resolved == nil {
					continue
				}
			}
			if len(resolved.Properties) > 0 {
				rootSchema = resolved
				break
			}
		}
	}
	return &schemaResult{
		RootSchema: rootSchema,
		Components: spec.Components.Schemas,
	}, nil
}

// findSchemaForGVK finds the schema in schemas that has
// x-kubernetes-group-version-kind matching gvk.
func findSchemaForGVK(
	schemas map[string]*openapispec.Schema,
	gvk schema.GroupVersionKind) *openapispec.Schema {

	for _, s := range schemas {
		if s == nil {
			continue
		}
		if gvkMatches(s.Extensions, gvk) {
			return s
		}
	}
	return nil
}

func gvkMatches(ext map[string]interface{}, gvk schema.GroupVersionKind) bool {
	raw, ok := ext["x-kubernetes-group-version-kind"]
	if !ok {
		return false
	}
	slice, ok := raw.([]interface{})
	if !ok || len(slice) == 0 {
		return false
	}
	for _, item := range slice {
		m, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		g, _ := m["group"].(string)
		v, _ := m["version"].(string)
		k, _ := m["kind"].(string)
		if g == gvk.Group && v == gvk.Version && k == gvk.Kind {
			return true
		}
	}
	return false
}

// resolveRef returns the schema pointed to by s.Ref from components, or nil.
// visited is used to avoid infinite recursion on cycles; pass nil to start.
func resolveRef(
	s *openapispec.Schema,
	components map[string]*openapispec.Schema,
	visited map[string]bool) *openapispec.Schema {

	if s == nil || components == nil {
		return nil
	}
	refStr := s.Ref.String()
	if refStr == "" {
		return s
	}
	name := refName(refStr)
	if name == "" {
		return nil
	}
	if visited != nil && visited[name] {
		return nil
	}
	if visited == nil {
		visited = make(map[string]bool)
	}
	visited[name] = true
	refSchema := components[name]
	if refSchema == nil {
		return nil
	}
	// The ref might point to another ref
	if refSchema.Ref.String() != "" {
		return resolveRef(refSchema, components, visited)
	}
	return refSchema
}

func refName(ref string) string {
	// ref is like "#/components/schemas/io.k8s.api.core.v1.Pod"
	const prefix = "#/components/schemas/"
	if strings.HasPrefix(ref, prefix) {
		return ref[len(prefix):]
	}
	const defPrefix = "#/definitions/"
	if strings.HasPrefix(ref, defPrefix) {
		return ref[len(defPrefix):]
	}
	return ""
}

func openAPISchemaType(s *openapispec.Schema) string {
	if s == nil || len(s.Type) == 0 {
		return ""
	}
	return s.Type[0]
}

const maxFlattenDepth = 10 // limit recursion when flattening allOf/$ref for property collection

// flattenSchemaProperties returns a map of property name -> schema and a list of required keys
// by following allOf and $ref. Kubernetes OpenAPI often has object schemas with type+properties
// (e.g. LabelSelector with matchExpressions, matchLabels) or allOf wrapping refs; flattening
// ensures we collect every property for merge and required-field lookup. Uses visited to avoid
// cycles and a depth limit.
func flattenSchemaProperties(s *openapispec.Schema, components map[string]*openapispec.Schema, visited map[string]bool, depth int) (map[string]openapispec.Schema, []string) {
	if s == nil || depth > maxFlattenDepth {
		return nil, nil
	}
	if visited == nil {
		visited = make(map[string]bool)
	}
	// Resolve $ref so we work with the actual schema
	if s.Ref.String() != "" && components != nil {
		name := refName(s.Ref.String())
		if name != "" && visited[name] {
			// Cycle: return shallow properties from components so we still get e.g. matchExpressions
			if refSchema := components[name]; refSchema != nil && len(refSchema.Properties) > 0 {
				out := make(map[string]openapispec.Schema)
				for k, v := range refSchema.Properties {
					out[k] = v
				}
				return out, refSchema.Required
			}
			return nil, nil
		}
		// Let ResolveRef set visited when it follows the ref; do not mark before or we block resolution.
		resolved := resolveRef(s, components, visited)
		if resolved == nil && name != "" {
			resolved = components[name]
		}
		if resolved == nil {
			return nil, nil
		}
		s = resolved
	}
	out := make(map[string]openapispec.Schema)
	var required []string
	if len(s.Properties) > 0 {
		for k, v := range s.Properties {
			out[k] = v
		}
		required = append(required, s.Required...)
	}
	for i := range s.AllOf {
		sub := &s.AllOf[i]
		subVisited := copyVisitedOpenAPI(visited)
		flattened, subReq := flattenSchemaProperties(sub, components, subVisited, depth+1)
		for k, v := range flattened {
			out[k] = v
		}
		required = append(required, subReq...)
	}
	return out, required
}

// findPropertyInAllOf returns the schema for key from s.Properties or from any allOf sub (recursively).
// Used to fill required fields when the merged schema missed a property (e.g. from a nested allOf).
func findPropertyInAllOf(s *openapispec.Schema, key string, components map[string]*openapispec.Schema, visited map[string]bool) *openapispec.Schema {
	if s == nil {
		return nil
	}
	if visited == nil {
		visited = make(map[string]bool)
	}
	// Resolve $ref so we look at the actual schema
	if s.Ref.String() != "" && components != nil {
		if resolved := resolveRef(s, components, visited); resolved != nil {
			s = resolved
		}
	}
	if prop, ok := s.Properties[key]; ok {
		return &prop
	}
	for i := range s.OneOf {
		if found := findPropertyInAllOf(&s.OneOf[i], key, components, visited); found != nil {
			return found
		}
	}
	for i := range s.AnyOf {
		if found := findPropertyInAllOf(&s.AnyOf[i], key, components, visited); found != nil {
			return found
		}
	}
	for i := range s.AllOf {
		sub := &s.AllOf[i]
		resolved := sub
		if sub.Ref.String() != "" && components != nil {
			resolved = resolveRef(sub, components, copyVisitedOpenAPI(visited))
			if resolved == nil {
				// Cycle: shallow look in the ref'd schema (no recurse to avoid infinite loop).
				if name := refName(sub.Ref.String()); name != "" {
					if refSchema := components[name]; refSchema != nil {
						if p, ok := refSchema.Properties[key]; ok {
							return &p
						}
						for j := range refSchema.AllOf {
							rsub := &refSchema.AllOf[j]
							if rsub.Ref.String() != "" {
								if n := refName(rsub.Ref.String()); n != "" && components[n] != nil {
									rsub = components[n]
								}
							}
							if rsub != nil {
								if p, ok := rsub.Properties[key]; ok {
									return &p
								}
								for k := range rsub.AllOf {
									rsub2 := &rsub.AllOf[k]
									if rsub2.Ref.String() != "" {
										if n := refName(rsub2.Ref.String()); n != "" && components[n] != nil {
											rsub2 = components[n]
										}
									}
									if rsub2 != nil {
										if p, ok := rsub2.Properties[key]; ok {
											return &p
										}
									}
								}
							}
						}
					}
				}
				continue
			}
		}
		if prop := findPropertyInAllOf(resolved, key, components, visited); prop != nil {
			return prop
		}
	}
	return nil
}

// openAPISchemaTypeWithRefAllOf returns the type of s after resolving $ref and allOf so
// list-map key fields (e.g. containerPort) get the correct type from the schema.
func openAPISchemaTypeWithRefAllOf(s *openapispec.Schema, components map[string]*openapispec.Schema, visited map[string]bool) string {
	if s == nil {
		return ""
	}
	resolved := s
	if s.Ref.String() != "" && components != nil {
		resolved = resolveRef(s, components, visited)
		if resolved == nil {
			return ""
		}
	}
	if len(resolved.Type) > 0 {
		return resolved.Type[0]
	}
	if resolved.Items != nil {
		return "array"
	}
	for i := range resolved.AllOf {
		sub := &resolved.AllOf[i]
		r := sub
		if sub.Ref.String() != "" && components != nil {
			r = resolveRef(sub, components, copyVisitedOpenAPI(visited))
			if r == nil {
				// Cycle: get type from the ref'd schema in components (shallow: no recurse to avoid infinite loop).
				if name := refName(sub.Ref.String()); name != "" {
					if refSchema := components[name]; refSchema != nil {
						if len(refSchema.Type) > 0 {
							return refSchema.Type[0]
						}
						if refSchema.Items != nil {
							return "array"
						}
						for j := range refSchema.AllOf {
							subR := &refSchema.AllOf[j]
							var resolved *openapispec.Schema
							if subR.Ref.String() != "" {
								resolved = resolveRef(subR, components, copyVisitedOpenAPI(visited))
								if resolved == nil {
									if n := refName(refSchema.AllOf[j].Ref.String()); n != "" && components[n] != nil {
										resolved = components[n]
									}
								}
							} else {
								resolved = subR
							}
							if resolved != nil {
								if len(resolved.Type) > 0 {
									return resolved.Type[0]
								}
								if resolved.Items != nil {
									return "array"
								}
							}
						}
					}
				}
				continue
			}
		}
		if len(r.Type) > 0 {
			return r.Type[0]
		}
		if r.Items != nil {
			return "array"
		}
	}
	return ""
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

// uniqueListMapKeyValue returns a unique value for a list-map key field so that each list entry
// gets a distinct value. The value is chosen from the field's schema type (integer/number → index,
// string → unique label, boolean → alternate, enum → pick by index). components/visited are used
// to resolve $ref and allOf so the type is taken from the schema.
func uniqueListMapKeyValue(itemSchema *openapispec.Schema, listKey string, i, seed int, r *rand.Rand, components map[string]*openapispec.Schema, visited map[string]bool) interface{} {
	if itemSchema == nil {
		return fmt.Sprintf("key-%d-%d", seed+i, r.Int())
	}
	item := itemSchema
	if itemSchema.Ref.String() != "" && components != nil {
		if resolved := resolveRef(itemSchema, components, copyVisitedOpenAPI(visited)); resolved != nil {
			item = resolved
		}
	}
	propSchema, ok := item.Properties[listKey]
	if !ok && len(item.AllOf) > 0 && components != nil {
		for ai := range item.AllOf {
			sub := &item.AllOf[ai]
			rsub := sub
			if sub.Ref.String() != "" {
				rsub = resolveRef(sub, components, copyVisitedOpenAPI(visited))
				if rsub == nil {
					continue
				}
			}
			if p, has := rsub.Properties[listKey]; has {
				propSchema = p
				ok = true
				break
			}
		}
	}
	if !ok {
		return fmt.Sprintf("key-%d-%d", seed+i, r.Int())
	}
	prop := &propSchema
	pt := openAPISchemaTypeWithRefAllOf(prop, components, visited)
	if pt == "" {
		pt = openAPISchemaType(prop)
	}
	switch pt {
	case "integer", "number":
		return float64(i)
	case "string":
		enum := propSchema.Enum
		if len(enum) == 0 {
			enum = enumFromAllOfOneOfAnyOfOpenAPI(&propSchema)
		}
		if len(enum) > 0 {
			return enum[i%len(enum)]
		}
		return dnsLabel(19, seed+i, r)
	case "boolean":
		return i%2 == 1
	default:
		return fmt.Sprintf("key-%d-%d", seed+i, r.Int())
	}
}

const maxFuzzDepth = 8 // limit recursion for nested objects/lists

// quantitySuffix matches valid Kubernetes quantity suffix (e.g. 1Gi, 100Mi, 1).
var quantitySuffix = regexp.MustCompile(`^[0-9]+(\.[0-9]+)?([KMGTPE]i?|[eE][+-]?[0-9]+)?$`)

// quantityFull matches full Kubernetes quantity pattern (optional sign, number, optional suffix).
var quantityFull = regexp.MustCompile(`^(\+|-)?(([0-9]+(\.[0-9]*)?)|(\.[0-9]+))(([KMGTPE]i)|[numkMGTPE]|([eE](\+|-)?(([0-9]+(\.[0-9]*)?)|(\.[0-9]+))))?$`)

// reasonPattern matches condition reason: CamelCase / [A-Za-z][A-Za-z0-9_,:]*[A-Za-z0-9_]
var reasonPattern = regexp.MustCompile(`^[A-Za-z]([A-Za-z0-9_,:]*[A-Za-z0-9_])?$`)

// sanitizeFuzzedStatus recurses into the status map and fixes invalid values using the schema's types
// (quantity, reason, numeric fields). When schema is nil, only reason-pattern fix is applied for backward compatibility.
func sanitizeFuzzedStatus(m map[string]interface{}, schema *openapispec.Schema, components map[string]*openapispec.Schema) {
	if schema == nil {
		schema = &openapispec.Schema{}
	}
	for k, v := range m {
		propSchema := schema.Properties[k]
		if propSchema.Ref.String() != "" && components != nil {
			if resolved := resolveRef(&propSchema, components, nil); resolved != nil {
				propSchema = *resolved
			}
		}
		switch vv := v.(type) {
		case map[string]interface{}:
			sub := &openapispec.Schema{}
			if propSchema.Properties != nil {
				sub = &propSchema
			}
			sanitizeFuzzedStatus(vv, sub, components)
		case []interface{}:
			var itemSchema *openapispec.Schema
			if propSchema.Items != nil {
				itemSchema = propSchema.Items.Schema
				if itemSchema == nil && len(propSchema.Items.Schemas) > 0 {
					itemSchema = &propSchema.Items.Schemas[0]
				}
			}
			sub := &openapispec.Schema{}
			if itemSchema != nil {
				sub = itemSchema
			}
			for _, el := range vv {
				if mm, ok := el.(map[string]interface{}); ok {
					sanitizeFuzzedStatus(mm, sub, components)
				}
			}
		case string:
			if openAPIIsQuantityField(&propSchema) && !quantityFull.MatchString(vv) {
				m[k] = "1Gi"
				continue
			}
			if openAPIIsReasonField(&propSchema) && !reasonPattern.MatchString(vv) {
				m[k] = "Ready"
				continue
			}
			// Schema says integer/number but value is string (e.g. from int-or-string enum)
			if openAPISchemaType(&propSchema) == "integer" || openAPISchemaType(&propSchema) == "number" {
				m[k] = float64(0)
				continue
			}
		case float64:
			if openAPISchemaType(&propSchema) == "integer" {
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

// defaultForRequiredField returns a validation-friendly default from the field's schema type/format.
// fieldName is used only for Kubernetes reference fields apiVersion and kind when schema does not provide a default.
// components/visited are used to resolve $ref and allOf so the type is taken from the schema.
func defaultForRequiredField(s *openapispec.Schema, fieldName string, seed int, r *rand.Rand, components map[string]*openapispec.Schema, visited map[string]bool) interface{} {
	// Kubernetes reference types require apiVersion and kind; use sensible defaults when required.
	switch strings.ToLower(fieldName) {
	case "apiversion":
		return "v1"
	case "kind":
		return "Object"
	}
	if s == nil {
		return dnsLabel(32, seed, r)
	}
	// Use a fresh visited map so ref resolution is not blocked by a sibling branch
	// (e.g. LabelSelector resolved under affinity would otherwise block resolution under volumes).
	typ := openAPISchemaTypeWithRefAllOf(s, components, make(map[string]bool))
	if typ == "" {
		typ = openAPISchemaType(s)
	}
	if typ == "" && s.Items != nil {
		typ = "array"
	}
	switch typ {
	case "string":
		if openAPIIsQuantityField(s) {
			return "1Gi"
		}
		if openAPIIsReasonField(s) {
			return "Ready"
		}
		if openAPIIsUUIDField(s) {
			return uuidV4(seed, r)
		}
		if s.Format != "" {
			if v := stringFromFormat(s.Format, seed, r); v != "" {
				return v
			}
		}
		maxL := int64(32)
		if s.MaxLength != nil && *s.MaxLength > 0 {
			maxL = *s.MaxLength
		}
		return dnsLabel(int(maxL), seed, r)
	case "boolean":
		return false
	case "integer", "number":
		val := float64(0)
		if s.Minimum != nil {
			val = *s.Minimum
		}
		return val
	case "array":
		return []interface{}{}
	case "object":
		return map[string]interface{}{}
	default:
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
	case "quantity":
		return "1Gi"
	default:
		return ""
	}
}

// openAPIIsQuantityField returns true if the schema represents a Kubernetes quantity (resource.Quantity) string.
func openAPIIsQuantityField(s *openapispec.Schema) bool {
	if s == nil {
		return false
	}
	if s.Format == "quantity" {
		return true
	}
	if s.Pattern != "" && (strings.Contains(s.Pattern, "KMGTPE") || strings.Contains(s.Pattern, "quantity") || strings.Contains(s.Pattern, "numkMGTPE")) {
		return true
	}
	return false
}

// openAPIIsReasonField returns true if the schema looks like a condition reason (CamelCase pattern).
func openAPIIsReasonField(s *openapispec.Schema) bool {
	if s == nil || s.Pattern == "" {
		return false
	}
	return strings.Contains(s.Pattern, "[A-Za-z0-9_])?$")
}

// openAPIIsUUIDField returns true if the schema represents a UUID string (format or pattern).
func openAPIIsUUIDField(s *openapispec.Schema) bool {
	if s == nil {
		return false
	}
	if s.Format == "uuid" || s.Format == "uuid3" || s.Format == "uuid4" || s.Format == "uuid5" {
		return true
	}
	if s.Pattern != "" && (strings.Contains(s.Pattern, "uuid") || strings.Contains(s.Pattern, "8-4-4-4-12")) {
		return true
	}
	return false
}

// stringMatchingPatternOpenAPI returns a string that satisfies the schema's length, format, and pattern.
// It uses only the field's schema type/format/pattern, not the field name.
func stringMatchingPatternOpenAPI(s *openapispec.Schema, seed int, r *rand.Rand) string {
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
	if maxL < minL {
		maxL = minL
	}
	if openAPIIsUUIDField(s) {
		return uuidV4(seed, r)
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
	if openAPIIsReasonField(s) {
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

// Server-Side Apply x-kubernetes-* extensions (see https://kubernetes.io/docs/reference/using-api/server-side-apply/)
// that affect fuzz generation:
//
//   - x-kubernetes-list-type (atomic | set | map)
//     atomic: list is replaced as a whole; no per-element uniqueness.
//     set: list elements must be unique (scalars or comparable); we dedupe by value and ensure object items have a unique identity field when possible.
//     map: list of objects with unique values for x-kubernetes-list-map-keys; we set those keys per index so entries are distinct.
//
//   - x-kubernetes-list-map-keys: field names that uniquely identify list entries when list-type=map. We assign unique values (e.g. index for numeric keys, unique string for name/key).
//
//   - x-kubernetes-map-type (atomic | granular): applies to maps and structs. atomic = whole map/struct replaced; granular = keys/fields managed separately. Affects merge only; we do not change generation shape.
//
//   - x-kubernetes-embedded-resource: object must include apiVersion, kind, metadata. We emit them when this is set.
//
//   - x-kubernetes-int-or-string: field may be int or string. We randomly pick one.
//
//   - x-kubernetes-preserve-unknown-fields: allows unknown fields; we do not add random unknown fields.
//
// fieldPath is the dot-separated path to the current schema from the resource root (e.g. "spec", "spec.priorityClassName").
// It is used to query fieldLookupFns for overrides per API Kind.
func fuzzFromOpenAPISchema(s *openapispec.Schema, components map[string]*openapispec.Schema, visited map[string]bool, seed int, depth int, gvk schema.GroupVersionKind, fieldPath string) interface{} {
	if depth > maxFuzzDepth {
		return nil
	}
	r := rand.New(rand.NewSource(int64(seed)))
	if s == nil {
		return nil
	}
	// Resolve $ref so we fuzz the actual schema
	if s.Ref.String() != "" {
		resolved := resolveRef(s, components, visited)
		if resolved != nil {
			s = resolved
		}
	}
	var allOfSource *openapispec.Schema // schema that had allOf, for looking up required-but-missing properties
	// Kubernetes OpenAPI v3 often uses allOf; merge Properties and Required from current schema and all subs so every required field has a type.
	if len(s.AllOf) > 0 {
		allOfSource = s
		merged := &openapispec.Schema{}
		requiredSet := make(map[string]bool)
		if len(s.Properties) > 0 {
			merged.Properties = make(map[string]openapispec.Schema)
			for k, v := range s.Properties {
				merged.Properties[k] = v
			}
			// Only require fields we have a schema for, so we never default a required field to string without a type.
			for _, req := range s.Required {
				if _, ok := s.Properties[req]; ok {
					requiredSet[req] = true
				}
			}
		}
		// Flatten each allOf sub (follows $ref and nested allOf; cycle-safe) so we get every property
		// (e.g. LabelSelector's matchExpressions, matchLabels from the OpenAPI structure).
		// Use a fresh visited per sub so one sub's refs don't block another from contributing properties.
		for i := range s.AllOf {
			sub := &s.AllOf[i]
			subVisited := make(map[string]bool)
			flattened, subRequired := flattenSchemaProperties(sub, components, subVisited, 0)
			if len(flattened) == 0 {
				continue
			}
			if merged.Properties == nil {
				merged.Properties = make(map[string]openapispec.Schema)
			}
			for k, v := range flattened {
				merged.Properties[k] = v
			}
			for _, req := range subRequired {
				if _, ok := flattened[req]; ok {
					requiredSet[req] = true
				}
			}
		}
		if len(merged.Properties) > 0 {
			for req := range requiredSet {
				merged.Required = append(merged.Required, req)
			}
			s = merged
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
		return enum[r.Intn(len(enum))]
	}
	if openAPIExtBool(s, "x-kubernetes-int-or-string") {
		if r.Intn(2) == 0 {
			return float64(r.Intn(1000))
		}
		return dnsLabel(19, seed, r)
	}
	typ := openAPISchemaType(s)
	if typ == "" && len(s.Properties) > 0 {
		typ = "object"
	}
	if typ == "" && s.Items != nil {
		typ = "array"
	}
	switch typ {
	case "string":
		str := stringMatchingPatternOpenAPI(s, seed, r)
		if str == "" {
			str = dnsLabel(32, seed, r)
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
		// Server expects integer for integer type; truncate so we don't send fractional part.
		if typ == "integer" {
			val = math.Trunc(val)
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
		listType := openAPIExtString(s, "x-kubernetes-list-type") // atomic | set | map
		listMapKeys := openAPIExtStrings(s, "x-kubernetes-list-map-keys")
		// atomic: no per-element uniqueness; list-map-keys apply only when listType=map.
		seen := make(map[string]bool)
		for i := int64(0); i < n; i++ {
			if itemSchema == nil {
				arr = append(arr, dnsLabel(32, seed+int(i), r))
				continue
			}
			if depth+1 > maxFuzzDepth {
				it := openAPISchemaTypeWithRefAllOf(itemSchema, components, make(map[string]bool))
				if it == "" {
					it = openAPISchemaType(itemSchema)
				}
				if it == "" && itemSchema.Items != nil {
					it = "array"
				}
				if it == "object" || (it == "" && len(itemSchema.Properties) > 0) {
					placeholder := map[string]interface{}{}
					arr = append(arr, placeholder)
					// Set list-map keys on placeholder so entries remain unique (no duplicate key values).
					if listType == "map" && len(listMapKeys) > 0 {
						for _, listKey := range listMapKeys {
							placeholder[listKey] = uniqueListMapKeyValue(itemSchema, listKey, int(i), seed, r, components, copyVisitedOpenAPI(visited))
						}
					}
				} else if it == "array" {
					arr = append(arr, []interface{}{})
				} else {
					arr = append(arr, dnsLabel(32, seed+int(i), r))
				}
				continue
			}
			item := fuzzFromOpenAPISchema(itemSchema, components, copyVisitedOpenAPI(visited), seed+int(i)*100, depth+1, gvk, fieldPath)
			if item == nil {
				it := openAPISchemaTypeWithRefAllOf(itemSchema, components, make(map[string]bool))
				if it == "" {
					it = openAPISchemaType(itemSchema)
				}
				if it == "" && itemSchema.Items != nil {
					it = "array"
				}
				if it == "object" || (it == "" && len(itemSchema.Properties) > 0) {
					item = map[string]interface{}{}
				} else if it == "array" {
					item = []interface{}{}
				} else {
					item = dnsLabel(32, seed+int(i), r)
				}
			}
			// x-kubernetes-list-type=map with x-kubernetes-list-map-keys: each key must be
			// unique per entry (see https://kubernetes.io/docs/reference/using-api/server-side-apply/).
			// Values are chosen from the field's schema type, not its name.
			if listType == "map" && len(listMapKeys) > 0 {
				if m, ok := item.(map[string]interface{}); ok {
					for _, listKey := range listMapKeys {
						m[listKey] = uniqueListMapKeyValue(itemSchema, listKey, int(i), seed, r, components, copyVisitedOpenAPI(visited))
					}
				}
			}
			// x-kubernetes-list-type=set: elements must be unique. For object items, ensure an identity field
			// is set so our dedupe (seen[key]) doesn't treat distinct objects as duplicates.
			if listType == "set" {
				if m, ok := item.(map[string]interface{}); ok {
					for _, idField := range []string{"name", "id", "key"} {
						if _, hasProp := itemSchema.Properties[idField]; hasProp {
							if _, set := m[idField]; !set {
								m[idField] = dnsLabel(19, seed+int(i), r)
							}
							break
						}
					}
				}
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
			// Skip only metadata when the object is not an embedded resource (e.g. top-level spec has no metadata).
			// Always include apiVersion and kind when present so reference types (class, image, policies) get required values.
			if propName == "metadata" && !openAPIExtBool(s, "x-kubernetes-embedded-resource") {
				continue
			}
			prop := propSchema
			childPath := pathJoin(fieldPath, propName)
			v := fuzzFromOpenAPISchema(&prop, components, copyVisitedOpenAPI(visited), seed+int(r.Int63n(100)), depth+1, gvk, childPath)
			if v == nil {
				if requiredSet[propName] {
					v = defaultForRequiredField(&propSchema, propName, seed, r, components, copyVisitedOpenAPI(visited))
				} else {
					propTyp := openAPISchemaTypeWithRefAllOf(&propSchema, components, copyVisitedOpenAPI(visited))
					if propTyp == "" {
						propTyp = openAPISchemaType(&propSchema)
					}
					if propTyp == "" && propSchema.Items != nil {
						propTyp = "array"
					}
					if propTyp == "object" || (propTyp == "" && (len(propSchema.Properties) > 0)) {
						v = map[string]interface{}{}
					} else if propTyp == "array" {
						v = []interface{}{}
					}
				}
			}
			if v != nil {
				if override, ok := fieldFuzzerOverride(gvk.Kind, childPath); ok {
					if override == nil {
						// Override says do not set this field.
						continue
					}
					v = override
				}
				out[propName] = v
			}
		}
		for req := range requiredSet {
			if _, ok := out[req]; ok {
				continue
			}
			if req == "metadata" {
				continue
			}
			reqPath := pathJoin(fieldPath, req)
			if override, ok := fieldFuzzerOverride(gvk.Kind, reqPath); ok {
				if override == nil {
					continue
				}
				out[req] = override
				continue
			}
			propSchema, inMerged := s.Properties[req]
			if !inMerged && allOfSource != nil {
				// Flatten allOf tree (same as merge) to find property schema when it was missed (e.g. matchExpressions).
				flattened, _ := flattenSchemaProperties(allOfSource, components, nil, 0)
				if p, ok := flattened[req]; ok {
					propSchema = p
					inMerged = true
				}
				if !inMerged {
					if found := findPropertyInAllOf(allOfSource, req, components, nil); found != nil {
						propSchema = *found
						inMerged = true
					}
				}
			}
			if inMerged {
				out[req] = defaultForRequiredField(&propSchema, req, seed, r, components, copyVisitedOpenAPI(visited))
			} else {
				out[req] = defaultForRequiredField(nil, req, seed, r, components, copyVisitedOpenAPI(visited))
			}
		}
		// Fix any string values that are quantity-typed per schema but not valid quantity strings.
		for key, v := range out {
			if propSchema, ok := s.Properties[key]; ok && openAPIIsQuantityField(&propSchema) {
				if str, ok := v.(string); ok && !quantityFull.MatchString(str) {
					out[key] = "1Gi"
				}
			}
		}
		// If we had to default a required field to string (no schema found), fix it when the schema says array/object.
		for key, v := range out {
			if _, ok := v.(string); !ok {
				continue
			}
			prop, inS := s.Properties[key]
			if !inS && allOfSource != nil {
				flattened, _ := flattenSchemaProperties(allOfSource, components, nil, 0)
				if p, ok := flattened[key]; ok {
					prop = p
					inS = true
				}
				if !inS {
					if found := findPropertyInAllOf(allOfSource, key, components, nil); found != nil {
						prop = *found
						inS = true
					}
				}
			}
			typ := openAPISchemaTypeWithRefAllOf(&prop, components, nil)
			if typ == "" {
				typ = openAPISchemaType(&prop)
			}
			if typ == "" && prop.Items != nil {
				typ = "array"
			}
			if typ == "array" {
				out[key] = []interface{}{}
			} else if typ == "object" {
				out[key] = map[string]interface{}{}
			}
		}
		return out
	}
	if len(s.Properties) > 0 {
		return fuzzFromOpenAPISchema(s, components, visited, seed, depth, gvk, fieldPath)
	}
	for i := range s.OneOf {
		sub := &s.OneOf[i]
		if openAPISchemaType(sub) == "object" && len(sub.Properties) > 0 {
			return fuzzFromOpenAPISchema(sub, components, copyVisitedOpenAPI(visited), seed, depth, gvk, fieldPath)
		}
		if len(sub.Properties) > 0 {
			return fuzzFromOpenAPISchema(sub, components, copyVisitedOpenAPI(visited), seed, depth, gvk, fieldPath)
		}
		if openAPISchemaType(sub) == "array" && sub.Items != nil {
			return fuzzFromOpenAPISchema(sub, components, copyVisitedOpenAPI(visited), seed, depth, gvk, fieldPath)
		}
		if openAPISchemaType(sub) == "integer" || openAPISchemaType(sub) == "number" {
			return fuzzFromOpenAPISchema(sub, components, copyVisitedOpenAPI(visited), seed, depth, gvk, fieldPath)
		}
	}
	for i := range s.AnyOf {
		sub := &s.AnyOf[i]
		if openAPISchemaType(sub) == "object" && len(sub.Properties) > 0 {
			return fuzzFromOpenAPISchema(sub, components, copyVisitedOpenAPI(visited), seed, depth, gvk, fieldPath)
		}
		if len(sub.Properties) > 0 {
			return fuzzFromOpenAPISchema(sub, components, copyVisitedOpenAPI(visited), seed, depth, gvk, fieldPath)
		}
		if openAPISchemaType(sub) == "array" && sub.Items != nil {
			return fuzzFromOpenAPISchema(sub, components, copyVisitedOpenAPI(visited), seed, depth, gvk, fieldPath)
		}
		if openAPISchemaType(sub) == "integer" || openAPISchemaType(sub) == "number" {
			return fuzzFromOpenAPISchema(sub, components, copyVisitedOpenAPI(visited), seed, depth, gvk, fieldPath)
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

// pathJoin returns parentPath.childName, or childName if parentPath is empty.
func pathJoin(parentPath, childName string) string {
	if parentPath == "" {
		return childName
	}
	return parentPath + "." + childName
}

// fieldFuzzerOverride returns the override value and true if the Kind has a
// lookup that handles this field path.
// If the override value is nil, the caller should not set the field.
// If the override is non-nil, use it as the value.
func fieldFuzzerOverride(kind, fieldPath string) (any, bool) {
	if fn := fieldFuzzerOverrideFns[kind]; fn != nil {
		return fn(fieldPath)
	}
	return nil, false
}

type fieldFuzzerOverrideFn func(fieldPath string) (any, bool)

var fieldFuzzerOverrideFns = map[string]fieldFuzzerOverrideFn{
	"Pod": func(fieldPath string) (any, bool) {
		switch fieldPath {
		case //"spec.priorityClassName",
			"spec.priority",
			"spec.preemptionPolicy",
			"spec.resources",
			"spec.resourceClaims",
			"spec.resourceClaimName",
			"spec.resourceClaimTemplateName",
			"spec.runtimeClassName",
			"spec.volumes",
			"spec.initContainers",
			"spec.ephemeralContainers",
			"spec.tolerations",
			"spec.nodeSelector",
			"spec.affinity",
			"spec.topologySpreadConstraints",
			"spec.securityContext",
			"spec.serviceAccountName",
			"spec.automountServiceAccountToken",
			"spec.dnsConfig",
			"spec.readinessGates",
			"spec.hostNetwork",
			"spec.hostAliases",
			"spec.nodeName",
			"spec.os",
			"spec.hostnameOverride",
			"status":
			return nil, true
		}
		return nil, false
	},
}
