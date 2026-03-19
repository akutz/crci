package benchmark

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	openapispec "k8s.io/kube-openapi/pkg/validation/spec"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// fibonacci returns the first n fibonacci numbers.
func fibonacci(n int) []int {
	var (
		seq    []int
		n1, n2 = 0, 1
	)

	for range n {
		nextTerm := n1 + n2
		n1 = n2
		n2 = nextTerm
		seq = append(seq, nextTerm)
	}

	return seq
}

// ResolveGroupVersionKind returns the api.WatchedResource for the given
// resource name.
func ResolveGroupVersionKind(
	resourceName string) (schema.GroupVersionKind, error) {

	sansWhiteSpace := strings.TrimSpace(resourceName)
	lower := strings.ToLower(sansWhiteSpace)
	switch lower {
	case "configmap", "configmaps":
		return schema.GroupVersionKind{
			Group:   "",
			Version: "v1",
			Kind:    "ConfigMap",
		}, nil
	case "secret", "secrets":
		return schema.GroupVersionKind{
			Group:   "",
			Version: "v1",
			Kind:    "Secret",
		}, nil
	case "pod", "pods":
		return schema.GroupVersionKind{
			Group:   "",
			Version: "v1",
			Kind:    "Pod",
		}, nil
	default:
		// Parse as group/version/kind or version/kind (core)
		return parseGVK(sansWhiteSpace)
	}
}

// parseGVK parses "version/kind" (core) or "group/version/kind" into a WatchedResource.
func parseGVK(s string) (schema.GroupVersionKind, error) {
	parts := strings.Split(s, "/")
	if len(parts) == 2 {
		return schema.GroupVersionKind{
			Group:   "",
			Version: parts[0],
			Kind:    parts[1],
		}, nil
	}
	if len(parts) == 3 {
		return schema.GroupVersionKind{
			Group:   parts[0],
			Version: parts[1],
			Kind:    parts[2],
		}, nil
	}
	return schema.GroupVersionKind{}, fmt.Errorf(
		"resource must be configmap, secret, pod, or group/version/kind (got %q)", s)
}

// findModuleRoot returns the directory containing go.mod (module root), or an error.
func findModuleRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("getwd: %w", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("no go.mod found in current or parent directories")
		}
		dir = parent
	}
}

// objectCreator creates a client.Object for the i-th fuzz resource in the given
// namespace.
type objectCreator func(namespace string, i int) client.Object

// objectCreatorForWatched returns an objectCreator that creates fuzz objects
// for the given watched resource. rootSchema and components are from the API
// server's OpenAPI v3; when nil, unstructured fuzz uses minimal spec.
func objectCreatorForWatched(
	opts BenchmarkCacheMemoryOptions) (objectCreator, error) {

	// Schema for fuzzing is discovered from the API server's OpenAPI v3, not
	// from CRD files.
	var (
		rootSchema *openapispec.Schema
		components map[string]*openapispec.Schema
		gvk        = opts.GroupVersionKind
	)
	schemaResult, err := FetchSchemaForGVK(opts.Config, gvk)
	if err != nil {
		fmt.Fprintf(
			os.Stderr,
			" (openapi schema: %v, fuzzing with minimal spec) ",
			err)
	} else {
		rootSchema = schemaResult.RootSchema
		components = schemaResult.Components
	}

	oc := func(ns string, i int) client.Object {
		return fuzzUnstructured(rootSchema, components, gvk, ns, i)
	}

	return func(ns string, i int) client.Object {
		return toUnstructured(oc(ns, i), gvk)
	}, nil
}

func toUnstructured(
	obj client.Object,
	gvk schema.GroupVersionKind) client.Object {

	if u, ok := obj.(*unstructured.Unstructured); ok {
		return u
	}
	data, err := runtime.DefaultUnstructuredConverter.ToUnstructured(
		obj.DeepCopyObject())
	if err != nil {
		panic(fmt.Errorf("to unstructured: %w", err))
	}
	u := &unstructured.Unstructured{Object: data}
	u.SetGroupVersionKind(gvk)
	return u
}

func ptr(b bool) *bool { return &b }
