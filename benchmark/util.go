package benchmark

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
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

// watchedResource returns the api.WatchedResource for the given resource name.
func watchedResource(resourceName string) (schema.GroupVersionKind, error) {
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

// loadCRDsFromPaths reads one or more CRD YAML files (each may contain multiple documents
// separated by ---) and returns the CustomResourceDefinition objects for envtest.
func loadCRDsFromPaths(paths []string) ([]*apiextensionsv1.CustomResourceDefinition, error) {
	scheme := kruntime.NewScheme()
	if err := apiextensionsv1.AddToScheme(scheme); err != nil {
		return nil, fmt.Errorf("add apiextensions scheme: %w", err)
	}
	codec := serializer.NewCodecFactory(scheme).UniversalDeserializer()

	var crds []*apiextensionsv1.CustomResourceDefinition
	moduleRoot, err := findModuleRoot()
	if err != nil {
		return nil, err
	}

	for _, path := range paths {
		path = strings.TrimSpace(path)
		if path == "" {
			continue
		}
		// Resolve relative to module root so -crds=file.yaml works when run from repo root.
		if !filepath.IsAbs(path) {
			path = filepath.Join(moduleRoot, path)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read CRD file %q: %w", path, err)
		}
		// Split by "---" for multi-document YAML.
		docs := splitYAMLDocuments(data)
		for _, doc := range docs {
			if len(strings.TrimSpace(doc)) == 0 {
				continue
			}
			obj, _, err := codec.Decode([]byte(doc), nil, nil)
			if err != nil {
				return nil, fmt.Errorf("decode CRD from %q: %w", path, err)
			}
			crd, ok := obj.(*apiextensionsv1.CustomResourceDefinition)
			if !ok {
				return nil, fmt.Errorf("object in %q is not a CRD: %T", path, obj)
			}
			crds = append(crds, crd)
		}
	}
	return crds, nil
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

func splitYAMLDocuments(data []byte) []string {
	const sep = "\n---"
	var out []string
	rest := string(data)
	for {
		i := strings.Index(rest, sep)
		if i < 0 {
			if strings.TrimSpace(rest) != "" {
				out = append(out, rest)
			}
			return out
		}
		doc := rest[:i]
		if strings.TrimSpace(doc) != "" {
			out = append(out, doc)
		}
		rest = rest[i+len(sep):]
	}
}

// objectCreator creates a client.Object for the i-th fuzz resource in the given
// namespace.
type objectCreator func(namespace string, i int) client.Object

// objectCreatorForWatched returns an objectCreator that creates fuzz objects
// for the given watched resource.
// gvk is used to understand registered types; crds (when non-nil) supply
// OpenAPIV3Schema for CRDs so fuzzUnstructured can generate schema-aware fuzz.
func objectCreatorForWatched(
	crds []*apiextensionsv1.CustomResourceDefinition,
	gvk schema.GroupVersionKind) (objectCreator, error) {

	var oc objectCreator

	switch {
	case gvk.Group == "" && gvk.Version == "v1" && gvk.Kind == "ConfigMap":
		oc = func(ns string, i int) client.Object { return fuzzConfigMap(ns, i) }
	case gvk.Group == "" && gvk.Version == "v1" && gvk.Kind == "Secret":
		oc = func(ns string, i int) client.Object { return fuzzSecret(ns, i) }
	case gvk.Group == "" && gvk.Version == "v1" && gvk.Kind == "Pod":
		oc = func(ns string, i int) client.Object { return fuzzPod(ns, i) }
	default:
		oc = func(ns string, i int) client.Object { return fuzzUnstructured(crds, gvk, ns, i) }
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
