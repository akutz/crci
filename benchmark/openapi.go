package benchmark

import (
	"fmt"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	openapi3 "k8s.io/client-go/openapi3"
	"k8s.io/client-go/rest"
	"k8s.io/kube-openapi/pkg/spec3"
	openapispec "k8s.io/kube-openapi/pkg/validation/spec"
)

// OpenAPISchemaResult holds the root schema for a GVK and the components map
// for resolving $ref when fuzzing.
type OpenAPISchemaResult struct {
	RootSchema *openapispec.Schema
	Components map[string]*openapispec.Schema
}

// FetchSchemaForGVK fetches the OpenAPI v3 schema for the given GVK from the
// API server. The CRD is still used to register the API (via envtest), but
// schema discovery for fuzzing comes from the server's published OpenAPI.
// See https://kubernetes.io/docs/concepts/overview/kubernetes-api/
func FetchSchemaForGVK(
	cfg *rest.Config,
	gvk schema.GroupVersionKind) (*OpenAPISchemaResult, error) {

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
		if specErr != nil {
			if i < 5 {
				time.Sleep(1 * time.Second)
			} else {
				return nil, fmt.Errorf(
					"fetch OpenAPI v3 for %s: %w", gv.String(), err)
			}
		}
	}

	if spec == nil || spec.Components == nil || spec.Components.Schemas == nil {
		return nil, fmt.Errorf("no schemas in OpenAPI v3 for %s", gv.String())
	}
	rootSchema := findSchemaForGVK(spec.Components.Schemas, gvk)
	if rootSchema == nil {
		return nil, fmt.Errorf("no schema found for %s in OpenAPI v3", gvk.String())
	}
	return &OpenAPISchemaResult{
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

// ResolveRef returns the schema pointed to by s.Ref from components, or nil.
// visited is used to avoid infinite recursion on cycles; pass nil to start.
func ResolveRef(
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
		return ResolveRef(refSchema, components, visited)
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
