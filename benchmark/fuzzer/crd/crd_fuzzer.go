package crd

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"time"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/google/uuid"

	"github.com/akutz/crci/benchmark/fuzzer"
)

const (
	maxFuzzDepth  = 24
	maxArrayElems = 12
	maxMapExtras  = 6
	extraKeyRunes = "abcdefghijklmnopqrstuvwxyz0123456789"
)

// New returns a new fuzzer that fuzzes the given GVK using a CRD.
func New(
	gvk schema.GroupVersionKind,
	crds []apiextensionsv1.CustomResourceDefinition) fuzzer.FuzzerFn {

	schema := schemaForGVK(gvk, crds)

	return func(obj client.Object, namespace string, i int) error {
		if schema == nil {
			return fmt.Errorf("failed to find crd for %s", gvk)
		}

		switch tObj := obj.(type) {
		case *unstructured.Unstructured:
			fuzzUnstructured(gvk, schema, tObj, namespace, i)
		default:
			return fmt.Errorf("unsupported object type: %T", obj)
		}
		return nil
	}
}

func schemaForGVK(
	gvk schema.GroupVersionKind,
	crds []apiextensionsv1.CustomResourceDefinition) *apiextensionsv1.JSONSchemaProps {

	for _, c := range crds {
		if c.Spec.Group == gvk.Group &&
			c.Spec.Names.Kind == gvk.Kind {
			for _, v := range c.Spec.Versions {
				if v.Name == gvk.Version && v.Schema != nil {
					return v.Schema.OpenAPIV3Schema
				}
			}
		}
	}
	return nil
}

func fuzzUnstructured(
	gvk schema.GroupVersionKind,
	root *apiextensionsv1.JSONSchemaProps,
	obj *unstructured.Unstructured,
	namespace string,
	seed int) {

	r := rand.New(rand.NewSource(int64(seed)))

	obj.SetNamespace(namespace)
	obj.SetName(fmt.Sprintf("%s-%d-%d", strings.ToLower(gvk.Kind), seed, r.Int()))
	obj.SetLabels(map[string]string{"bench": "crci", "seed": fmt.Sprintf("%d", seed)})

	if root != nil {
		sr := &schemaRoot{root: root}
		body := fuzzJSONValue(root, r, sr, 0)
		if m, ok := body.(map[string]any); ok {
			for k, v := range m {
				obj.Object[k] = v
			}
		}
	}
}

// schemaRoot carries the CRD root schema for $ref / definitions resolution.
type schemaRoot struct {
	root *apiextensionsv1.JSONSchemaProps
}

func (sr *schemaRoot) definitions() apiextensionsv1.JSONSchemaDefinitions {
	if sr == nil || sr.root == nil {
		return nil
	}
	return sr.root.Definitions
}

func deref(s *apiextensionsv1.JSONSchemaProps, defs apiextensionsv1.JSONSchemaDefinitions) *apiextensionsv1.JSONSchemaProps {
	if s == nil {
		return nil
	}
	if s.Ref == nil || *s.Ref == "" {
		return s
	}
	ref := *s.Ref
	const pfx = "#/definitions/"
	if strings.HasPrefix(ref, pfx) {
		name := strings.TrimPrefix(ref, pfx)
		if def, ok := defs[name]; ok {
			return deref(ptrProps(def), defs)
		}
	}
	return s
}

func ptrProps(p apiextensionsv1.JSONSchemaProps) *apiextensionsv1.JSONSchemaProps {
	pp := p
	return &pp
}

func mergedSchema(s *apiextensionsv1.JSONSchemaProps) apiextensionsv1.JSONSchemaProps {
	if s == nil {
		return apiextensionsv1.JSONSchemaProps{}
	}
	out := *s
	subs := out.AllOf
	out.AllOf = nil
	for i := range subs {
		mergeInto(&out, &subs[i])
	}
	return out
}

func mergeInto(dst *apiextensionsv1.JSONSchemaProps, src *apiextensionsv1.JSONSchemaProps) {
	if src == nil {
		return
	}
	if src.Type != "" {
		dst.Type = src.Type
	}
	if len(src.Properties) > 0 {
		if dst.Properties == nil {
			dst.Properties = map[string]apiextensionsv1.JSONSchemaProps{}
		}
		for k, v := range src.Properties {
			if existing, ok := dst.Properties[k]; ok {
				merged := existing
				mergeInto(&merged, &v)
				dst.Properties[k] = merged
			} else {
				dst.Properties[k] = v
			}
		}
	}
	dst.Required = unionStrings(dst.Required, src.Required)
	if src.Minimum != nil {
		if dst.Minimum == nil || *src.Minimum > *dst.Minimum {
			v := *src.Minimum
			dst.Minimum = &v
			dst.ExclusiveMinimum = src.ExclusiveMinimum
		}
	}
	if src.Maximum != nil {
		if dst.Maximum == nil || *src.Maximum < *dst.Maximum {
			v := *src.Maximum
			dst.Maximum = &v
			dst.ExclusiveMaximum = src.ExclusiveMaximum
		}
	}
	if src.MinLength != nil {
		if dst.MinLength == nil || *src.MinLength > *dst.MinLength {
			v := *src.MinLength
			dst.MinLength = &v
		}
	}
	if src.MaxLength != nil {
		if dst.MaxLength == nil || *src.MaxLength < *dst.MaxLength {
			v := *src.MaxLength
			dst.MaxLength = &v
		}
	}
	if src.Pattern != "" {
		dst.Pattern = src.Pattern
	}
	if len(src.Enum) > 0 {
		dst.Enum = src.Enum
	}
	if src.Items != nil {
		dst.Items = src.Items
	}
	if src.Default != nil {
		dst.Default = src.Default
	}
	if src.Format != "" {
		dst.Format = src.Format
	}
	if src.Nullable {
		dst.Nullable = true
	}
	if src.XEmbeddedResource {
		dst.XEmbeddedResource = true
	}
	if src.XIntOrString {
		dst.XIntOrString = true
	}
	if src.XPreserveUnknownFields != nil && *src.XPreserveUnknownFields {
		t := true
		dst.XPreserveUnknownFields = &t
	}
	if src.XListType != nil {
		dst.XListType = src.XListType
	}
	if len(src.XListMapKeys) > 0 {
		dst.XListMapKeys = append([]string(nil), src.XListMapKeys...)
	}
	if src.AdditionalProperties != nil {
		dst.AdditionalProperties = src.AdditionalProperties
	}
	for _, sub := range src.AllOf {
		mergeInto(dst, &sub)
	}
}

func unionStrings(a, b []string) []string {
	seen := map[string]struct{}{}
	var out []string
	for _, s := range a {
		if _, ok := seen[s]; !ok {
			seen[s] = struct{}{}
			out = append(out, s)
		}
	}
	for _, s := range b {
		if _, ok := seen[s]; !ok {
			seen[s] = struct{}{}
			out = append(out, s)
		}
	}
	return out
}

func fuzzJSONValue(s *apiextensionsv1.JSONSchemaProps, r *rand.Rand, sr *schemaRoot, depth int) any {
	if depth > maxFuzzDepth {
		return nil
	}
	s = deref(s, sr.definitions())
	if s == nil {
		return nil
	}

	if len(s.OneOf) > 0 {
		ch := mergedSchema(&s.OneOf[r.Intn(len(s.OneOf))])
		base := mergedSchema(s)
		base.OneOf = nil
		base.AnyOf = nil
		mergeInto(&base, &ch)
		return fuzzConcrete(&base, r, sr, depth)
	}
	if len(s.AnyOf) > 0 {
		ch := mergedSchema(&s.AnyOf[r.Intn(len(s.AnyOf))])
		base := mergedSchema(s)
		base.OneOf = nil
		base.AnyOf = nil
		mergeInto(&base, &ch)
		return fuzzConcrete(&base, r, sr, depth)
	}

	sEff := mergedSchema(s)
	return fuzzConcrete(&sEff, r, sr, depth)
}

func fuzzConcrete(s *apiextensionsv1.JSONSchemaProps, r *rand.Rand, sr *schemaRoot, depth int) any {
	if s.Default != nil && r.Float32() < 0.35 {
		if v, ok := jsonToAny(s.Default); ok {
			return v
		}
	}
	if s.Nullable && r.Intn(5) == 0 {
		return nil
	}
	if s.XIntOrString {
		cp := *s
		cp.XIntOrString = false
		if r.Intn(2) == 0 {
			cp.Type = "integer"
			return fuzzInteger(&cp, r)
		}
		cp.Type = "string"
		return fuzzString(&cp, r)
	}
	if s.XEmbeddedResource {
		return fuzzEmbeddedObject(s, r, sr, depth)
	}
	if len(s.Enum) > 0 {
		ch := s.Enum[r.Intn(len(s.Enum))]
		if v, ok := jsonToAny(&ch); ok {
			return v
		}
	}

	preserve := s.XPreserveUnknownFields != nil && *s.XPreserveUnknownFields
	if preserve && s.Type == "" && len(s.Properties) == 0 && s.Items == nil {
		return randomJSON(r, depth+1)
	}
	if preserve && s.Type == "object" && len(s.Properties) == 0 && s.AdditionalProperties == nil {
		return randomJSONObject(r, depth+1)
	}

	typ := s.Type
	if typ == "" {
		switch {
		case len(s.Properties) > 0 || s.AdditionalProperties != nil:
			typ = "object"
		case s.Items != nil:
			typ = "array"
		}
	}

	switch typ {
	case "object", "":
		if len(s.Properties) > 0 || s.AdditionalProperties != nil {
			return fuzzObject(s, r, sr, depth)
		}
		if typ == "object" {
			return map[string]any{}
		}
		return nil
	case "array":
		return fuzzArray(s, r, sr, depth)
	case "string":
		return fuzzString(s, r)
	case "integer":
		return fuzzInteger(s, r)
	case "number":
		return fuzzNumber(s, r)
	case "boolean":
		return r.Intn(2) == 0
	default:
		return nil
	}
}

func fuzzEmbeddedObject(s *apiextensionsv1.JSONSchemaProps, r *rand.Rand, sr *schemaRoot, depth int) map[string]any {
	out := make(map[string]any)
	for k, sub := range s.Properties {
		out[k] = fuzzJSONValue(&sub, r, sr, depth+1)
	}
	return out
}

func fuzzObject(s *apiextensionsv1.JSONSchemaProps, r *rand.Rand, sr *schemaRoot, depth int) map[string]any {
	out := make(map[string]any)
	required := map[string]struct{}{}
	for _, k := range s.Required {
		required[k] = struct{}{}
	}

	for name, sub := range s.Properties {
		_, must := required[name]
		if !must && r.Float32() > 0.55 {
			continue
		}
		out[name] = fuzzJSONValue(&sub, r, sr, depth+1)
	}

	schemaForExtra, allow := additionalPropsSchema(s.AdditionalProperties)
	if allow && schemaForExtra != nil {
		n := r.Intn(maxMapExtras + 1)
		for i := 0; i < n; i++ {
			out[randomMapKey(r)] = fuzzJSONValue(schemaForExtra, r, sr, depth+1)
		}
	} else if allow && schemaForExtra == nil {
		n := r.Intn(maxMapExtras + 1)
		for i := 0; i < n; i++ {
			out[randomMapKey(r)] = randomJSON(r, depth+1)
		}
	}

	if s.MinProperties != nil && int64(len(out)) < *s.MinProperties {
		// Add synthetic entries to satisfy minProperties when additionalProperties allow it.
		if allow {
			for int64(len(out)) < *s.MinProperties {
				k := randomMapKey(r)
				if _, exists := out[k]; exists {
					continue
				}
				if schemaForExtra != nil {
					out[k] = fuzzJSONValue(schemaForExtra, r, sr, depth+1)
				} else {
					out[k] = randomJSON(r, depth+1)
				}
			}
		}
	}
	return out
}

func additionalPropsSchema(ap *apiextensionsv1.JSONSchemaPropsOrBool) (*apiextensionsv1.JSONSchemaProps, bool) {
	if ap == nil {
		return nil, false
	}
	if ap.Schema != nil {
		return ap.Schema, true
	}
	return nil, ap.Allows
}

func randomMapKey(r *rand.Rand) string {
	var b strings.Builder
	n := 3 + r.Intn(8)
	for i := 0; i < n; i++ {
		b.WriteByte(extraKeyRunes[r.Intn(len(extraKeyRunes))])
	}
	return b.String()
}

func fuzzArray(s *apiextensionsv1.JSONSchemaProps, r *rand.Rand, sr *schemaRoot, depth int) any {
	if s.Items == nil {
		return []any{}
	}
	n := pickArrayLength(r, s.MinItems, s.MaxItems)
	var elems []any

	if len(s.Items.JSONSchemas) > 0 {
		for i := 0; i < n; i++ {
			idx := i % len(s.Items.JSONSchemas)
			sub := s.Items.JSONSchemas[idx]
			elems = append(elems, fuzzJSONValue(&sub, r, sr, depth+1))
		}
		return elems
	}
	if s.Items.Schema == nil {
		return []any{}
	}
	itemSch := s.Items.Schema
	for i := 0; i < n; i++ {
		elem := fuzzJSONValue(itemSch, r, sr, depth+1)
		if m, ok := elem.(map[string]any); ok && s.XListType != nil && *s.XListType == "map" {
			for _, keyField := range s.XListMapKeys {
				m[keyField] = fmt.Sprintf("%d-%d-%s", i, r.Int63(), keyField)
			}
		}
		elems = append(elems, elem)
	}
	if s.UniqueItems && len(elems) > 1 {
		seen := map[string]struct{}{}
		for i := range elems {
			for {
				k := fmt.Sprintf("%v", elems[i])
				if _, dup := seen[k]; !dup {
					seen[k] = struct{}{}
					break
				}
				elems[i] = fuzzJSONValue(itemSch, r, sr, depth+2)
			}
		}
	}
	return elems
}

func pickArrayLength(r *rand.Rand, minItems, maxItems *int64) int {
	lo := 0
	if minItems != nil {
		lo = int(*minItems)
		if lo < 0 {
			lo = 0
		}
	}
	if lo > maxArrayElems {
		return maxArrayElems
	}
	hi := maxArrayElems
	if maxItems != nil {
		hi = int(*maxItems)
		if hi > maxArrayElems {
			hi = maxArrayElems
		}
	}
	if hi < lo {
		return lo
	}
	if lo == hi {
		return lo
	}
	return lo + r.Intn(hi-lo+1)
}

func fuzzString(s *apiextensionsv1.JSONSchemaProps, r *rand.Rand) string {
	if len(s.Enum) > 0 {
		ch := s.Enum[r.Intn(len(s.Enum))]
		if v, ok := jsonToAny(&ch); ok {
			if str, ok := v.(string); ok {
				return str
			}
			return fmt.Sprint(v)
		}
	}
	if s.Format != "" {
		if v := fuzzByFormat(s.Format, r); v != "" {
			return clampStringLen(v, s.MinLength, s.MaxLength, r)
		}
	}
	minL, maxL := int64(1), int64(24)
	if s.MinLength != nil {
		minL = *s.MinLength
	}
	if minL < 0 {
		minL = 0
	}
	if s.MaxLength != nil {
		maxL = *s.MaxLength
	}
	if maxL < minL {
		maxL = minL
	}
	if maxL > 256 {
		maxL = 256
	}
	length := int(minL)
	if maxL > minL {
		length += r.Intn(int(maxL-minL) + 1)
	}
	var b strings.Builder
	for b.Len() < length {
		b.WriteByte('a' + byte(r.Intn(26)))
	}
	return b.String()
}

func fuzzByFormat(format string, r *rand.Rand) string {
	switch format {
	case "uuid", "uuid4":
		return uuid.New().String()
	case "date":
		return time.Now().UTC().Format("2006-01-02")
	case "date-time", "datetime":
		return time.Now().UTC().Format(time.RFC3339Nano)
	case "duration":
		return (time.Duration(r.Intn(1000)) * time.Millisecond).String()
	case "hostname":
		return fmt.Sprintf("h%d.example.internal", r.Intn(1_000_000))
	case "ipv4":
		return fmt.Sprintf("10.%d.%d.%d", r.Intn(256), r.Intn(256), r.Intn(256))
	case "ipv6":
		return "2001:db8::1"
	case "uri":
		return fmt.Sprintf("https://example.test/%d", r.Intn(1_000_000))
	case "byte":
		buf := make([]byte, 8)
		_, _ = r.Read(buf)
		return base64.StdEncoding.EncodeToString(buf)
	case "email":
		return fmt.Sprintf("u%d@example.test", r.Intn(1_000_000))
	case "int64":
		return fmt.Sprintf("%d", r.Int63())
	case "password", "binary":
		return fuzzString(&apiextensionsv1.JSONSchemaProps{MinLength: int64Ptr(8), MaxLength: int64Ptr(32)}, r)
	default:
		return ""
	}
}

func clampStringLen(s string, minL, maxL *int64, r *rand.Rand) string {
	out := s
	if minL != nil && int64(len(out)) < *minL {
		pad := fuzzString(&apiextensionsv1.JSONSchemaProps{MinLength: int64Ptr(*minL - int64(len(out))), MaxLength: int64Ptr(*minL - int64(len(out)))}, r)
		out = out + pad
	}
	if maxL != nil && int64(len(out)) > *maxL {
		if *maxL <= 0 {
			return ""
		}
		out = out[:*maxL]
	}
	return out
}

func fuzzInteger(s *apiextensionsv1.JSONSchemaProps, r *rand.Rand) int64 {
	var lo, hi int64 = -1 << 20, 1 << 20
	if s.Minimum != nil {
		lo = int64(math.Ceil(*s.Minimum))
		if s.ExclusiveMinimum {
			lo++
		}
	}
	if s.Maximum != nil {
		hi = int64(math.Floor(*s.Maximum))
		if s.ExclusiveMaximum {
			hi--
		}
	}
	if hi < lo {
		return lo
	}
	v := lo
	if hi > lo {
		v = lo + r.Int63n(hi-lo+1)
	}
	if s.MultipleOf != nil && *s.MultipleOf != 0 {
		m := int64(*s.MultipleOf)
		if m != 0 {
			v = (v / m) * m
			if v < lo {
				v += m
			}
		}
	}
	return v
}

func fuzzNumber(s *apiextensionsv1.JSONSchemaProps, r *rand.Rand) float64 {
	lo, hi := -1e6, 1e6
	if s.Minimum != nil {
		lo = *s.Minimum
		if s.ExclusiveMinimum {
			lo = math.Nextafter(lo, hi)
		}
	}
	if s.Maximum != nil {
		hi = *s.Maximum
		if s.ExclusiveMaximum {
			hi = math.Nextafter(hi, lo)
		}
	}
	if hi < lo {
		return lo
	}
	return lo + r.Float64()*(hi-lo)
}

func randomJSON(r *rand.Rand, depth int) any {
	if depth > maxFuzzDepth {
		return nil
	}
	switch r.Intn(6) {
	case 0:
		return nil
	case 1:
		return r.Intn(2) == 0
	case 2:
		return r.Int63()
	case 3:
		return fuzzString(&apiextensionsv1.JSONSchemaProps{MinLength: int64Ptr(1), MaxLength: int64Ptr(12)}, r)
	case 4:
		return randomJSONArray(r, depth+1)
	default:
		return randomJSONObject(r, depth+1)
	}
}

func randomJSONArray(r *rand.Rand, depth int) []any {
	n := r.Intn(5)
	var out []any
	for i := 0; i < n; i++ {
		out = append(out, randomJSON(r, depth))
	}
	return out
}

func randomJSONObject(r *rand.Rand, depth int) map[string]any {
	out := make(map[string]any)
	n := r.Intn(maxMapExtras + 1)
	for i := 0; i < n; i++ {
		out[randomMapKey(r)] = randomJSON(r, depth)
	}
	return out
}

func jsonToAny(j *apiextensionsv1.JSON) (any, bool) {
	if j == nil || len(j.Raw) == 0 {
		return nil, false
	}
	var v any
	if err := json.Unmarshal(j.Raw, &v); err != nil {
		return nil, false
	}
	return v, true
}

func int64Ptr(v int64) *int64 {
	return &v
}
