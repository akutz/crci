package benchmark

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/yaml"

	crciController "github.com/akutz/crci/controller"
)

type OutputFormat string

const (
	OutputFormatCSV      OutputFormat = "csv"
	OutputFormatMarkdown OutputFormat = "markdown"
	OutputFormatText     OutputFormat = "text"
)

// BenchmarkCacheMemoryOptions configures a cache memory benchmark run.
type BenchmarkCacheMemoryOptions struct {
	// ResourceName is the Kubernetes resource to test: configmap, secret, pod,
	// or group/version/kind.
	ResourceName string

	// CRDs is a list of paths to CRD YAML files to load into the envtest K8s
	// endpoint.
	CRDs []string

	// OutputFormat is the format of the output.
	OutputFormat OutputFormat

	// Fibonacci is the number of fibonacci numbers to use.
	Fibonacci int

	// FibonacciPrint is true to print the fibonacci sequence to the console and
	// exit.
	FibonacciPrint bool
}

// BenchmarkCacheMemory runs the controller under envtest, loads N resources
// with fuzz-style data, waits for cache sync, and reports controller-runtime
// cache memory (heap in use). After all N runs, a derived set of summaries are
// printed.
func BenchmarkCacheMemory(
	ctx context.Context,
	opts BenchmarkCacheMemoryOptions) error {

	var (
		runs []heapRun
		fib  = fibonacci(opts.Fibonacci)
	)

	strFib := make([]string, len(fib))
	for i, n := range fib {
		strFib[i] = strconv.Itoa(n)
	}

	fmt.Fprintf(
		os.Stderr,
		"* Running benchmarks for Fibonacci sequence: %s\n",
		strings.Join(strFib, ", "))

	if opts.FibonacciPrint {
		return nil
	}

	for _, n := range fib {
		fmt.Fprintf(os.Stderr, "* %d objects...", n)
		r, err := benchmarkCacheMemoryN(ctx, opts, n)
		if err != nil {
			return fmt.Errorf("n=%d: %w", n, err)
		}
		runs = append(runs, heapRun{n: n, r: r})
		fmt.Fprintf(
			os.Stderr,
			"heap_mib=%.2f, yaml_kib=%.2f\n",
			r.bytesHeap/1024/1024,
			r.bytesYAML/1024)
	}

	if err := printResults(runs, opts.OutputFormat); err != nil {
		return fmt.Errorf("failed to print results: %w", err)
	}

	return nil
}

// benchmarkCacheMemoryN starts envtest and the controller, creates N fuzz
// resources, waits for cache sync, and reports heap memory (proxy for cache
// footprint).
func benchmarkCacheMemoryN(
	ctx context.Context,
	opts BenchmarkCacheMemoryOptions,
	n int) (_ benchmarkCacheMemoryResult, retErr error) {

	var (
		env  envtest.Environment
		crds []*apiextensionsv1.CustomResourceDefinition
	)

	if len(opts.CRDs) > 0 {
		var err error
		crds, err = loadCRDsFromPaths(opts.CRDs)
		if err != nil {
			return benchmarkCacheMemoryResult{}, fmt.Errorf("load CRDs: %w", err)
		}
		env.CRDs = crds
	}

	cfg, err := env.Start()
	if err != nil {
		return benchmarkCacheMemoryResult{}, fmt.Errorf("envtest start: %w", err)
	}
	defer func() {
		if err := env.Stop(); err != nil {
			err = fmt.Errorf("failed to stop envtest: %w", err)
			if retErr != nil {
				retErr = errors.Join(retErr, err)
			} else {
				retErr = err
			}
		}
	}()

	mgr, err := manager.New(cfg, manager.Options{})
	if err != nil {
		return benchmarkCacheMemoryResult{}, fmt.Errorf("create manager: %w", err)
	}

	gvk, err := watchedResource(opts.ResourceName)
	if err != nil {
		return benchmarkCacheMemoryResult{}, fmt.Errorf("resource: %w", err)
	}
	createObj, err := objectCreatorForWatched(crds, gvk)
	if err != nil {
		return benchmarkCacheMemoryResult{}, fmt.Errorf("object creator: %w", err)
	}

	gvkStr := ""
	if gvk.Group == "" {
		gvkStr = fmt.Sprintf("%s/%s", gvk.Version, gvk.Kind)
	} else {
		gvkStr = fmt.Sprintf("%s/%s/%s", gvk.Group, gvk.Version, gvk.Kind)
	}
	fmt.Fprintf(os.Stderr, "gvk=%s...", gvkStr)

	crciController.SkipNameValidation = ptr(true)
	if err := crciController.AddToManager(mgr, gvk); err != nil {
		return benchmarkCacheMemoryResult{}, fmt.Errorf("add controller: %w", err)
	}

	mgrCtx, mgrCancel := context.WithCancel(ctx)
	go func() {
		_ = mgr.Start(mgrCtx)
	}()
	defer mgrCancel()

	k8sClient := mgr.GetClient()

	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "bench-ns"}}
	if err := k8sClient.Create(ctx, ns); err != nil {
		return benchmarkCacheMemoryResult{}, fmt.Errorf("create namespace: %w", err)
	}

	if !mgr.GetCache().WaitForCacheSync(ctx) {
		return benchmarkCacheMemoryResult{}, fmt.Errorf("cache sync wait failed")
	}

	objs := make([]client.Object, n)
	for i := 0; i < n; i++ {
		objs[i] = createObj("bench-ns", i)
	}

	var bytesYAML float64
	for i := range objs {
		data, _ := yaml.Marshal(objs[i])
		bytesYAML += float64(len(data))
	}

	// Check if the object schema has a status field.
	hasStatus := false
	_, ok, err := unstructured.NestedFieldNoCopy(objs[0].(*unstructured.Unstructured).Object, "status")
	if err != nil {
		return benchmarkCacheMemoryResult{}, fmt.Errorf("get status: %w", err)
	}
	if ok {
		hasStatus = true
	}

	var objStatuses []any

	if hasStatus {
		objStatuses = make([]any, n)
		for i, obj := range objs {
			status, ok, err := unstructured.NestedFieldCopy(obj.(*unstructured.Unstructured).Object, "status")
			if err != nil {
				return benchmarkCacheMemoryResult{}, fmt.Errorf("get status: %w", err)
			}
			if !ok {
				return benchmarkCacheMemoryResult{}, fmt.Errorf("status not found")
			}
			objStatuses[i] = status
		}
	}

	// Load N resources with fuzz-style data (unique names, varied labels/data)
	for i := 0; i < len(objs); i++ {
		if err := k8sClient.Create(ctx, objs[i]); err != nil {
			return benchmarkCacheMemoryResult{}, fmt.Errorf("create resource %d: %w", i, err)
		}
		if hasStatus {
			unstructured.SetNestedField(
				objs[i].(*unstructured.Unstructured).Object,
				objStatuses[i],
				"status")
			if err := k8sClient.Status().Update(ctx, objs[i]); err != nil {
				return benchmarkCacheMemoryResult{}, fmt.Errorf("update status %d: %w", i, err)
			}
		}
	}

	if !mgr.GetCache().WaitForCacheSync(ctx) {
		return benchmarkCacheMemoryResult{}, fmt.Errorf("cache sync after create failed")
	}

	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	bytesHeap := float64(m.Alloc)

	return benchmarkCacheMemoryResult{
		bytesHeap: bytesHeap,
		bytesYAML: bytesYAML,
	}, nil
}
