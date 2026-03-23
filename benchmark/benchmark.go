package benchmark

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/yaml"

	"github.com/akutz/crci/benchmark/fuzzer"
	fuzzerOpenAPI "github.com/akutz/crci/benchmark/fuzzer/openapi"
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
	// Pause is true to pause the program after the API server is started.
	Pause bool

	// KubeConfig is the path to a kubeconfig file to use for the benchmark.
	KubeConfig string

	// Config is the Kubernetes API server configuration.
	Config *rest.Config

	// GroupVersionKind is the Kubernetes group, version, and kind of the
	// resource to test.
	GroupVersionKind schema.GroupVersionKind

	// OutputFormat is the format of the output.
	OutputFormat OutputFormat

	// Fibonacci is the number of fibonacci numbers to use.
	Fibonacci int

	// DryRun is true to print the benchmark summary and exit.
	DryRun bool

	// PrintYAML is true to print the YAML of one of the generated objects.
	PrintYAML bool
}

// BenchmarkCacheMemory runs the controller under envtest, loads N resources
// with fuzz-style data, waits for cache sync, and reports controller-runtime
// cache memory (heap in use). After all N runs, a derived set of summaries are
// printed.
func BenchmarkCacheMemory(
	ctx context.Context,
	opts BenchmarkCacheMemoryOptions) error {

	fmt.Fprintf(os.Stderr, "* Running benchmarks for:\n")
	fmt.Fprintf(os.Stderr, "  * kubeconfig: %s\n", opts.KubeConfig)
	fmt.Fprintf(os.Stderr, "  * hostname:   %s\n", opts.Config.Host)

	fib := fibonacci(opts.Fibonacci)
	strFib := make([]string, len(fib))
	for i, n := range fib {
		strFib[i] = strconv.Itoa(n)
	}
	fmt.Fprintf(
		os.Stderr,
		"  * fibonacci sequence: %s\n",
		strings.Join(strFib, ", "))

	var (
		gvkStr string
		gvk    = opts.GroupVersionKind
	)
	if opts.GroupVersionKind.Group == "" {
		gvkStr = fmt.Sprintf("%s/%s", gvk.Version, gvk.Kind)
	} else {
		gvkStr = fmt.Sprintf("%s/%s/%s", gvk.Group, gvk.Version, gvk.Kind)
	}
	fmt.Fprintf(
		os.Stderr,
		"  * groupVersionKind: %s\n",
		gvkStr)

	createObj := getObjectCreator(opts.GroupVersionKind)
	fuzzObj := fuzzerOpenAPI.New(opts.Config, opts.GroupVersionKind)

	if opts.PrintYAML {
		obj := createObj()
		fuzzObj(obj, "", 0)
		data, _ := yaml.Marshal(obj)
		fmt.Fprintf(os.Stderr, "  * example object:\n")
		fmt.Fprintf(os.Stderr, "    ---\n")
		for l := range bytes.Lines(data) {
			fmt.Fprintf(os.Stderr, "    %s", string(l))
		}
		fmt.Fprintf(os.Stderr, "    ---\n")
	}

	if opts.DryRun {
		return nil
	}

	fmt.Fprintln(os.Stderr)

	if opts.Pause {
		fmt.Fprintf(os.Stderr, "Press Enter to continue...")
		if _, err := bufio.NewReader(os.Stdin).ReadString('\n'); err != nil {
			return fmt.Errorf("failed to read stdin: %w", err)
		}
		fmt.Fprintln(os.Stderr)
	}

	var runs []heapRun
	for _, n := range fib {
		fmt.Fprintf(os.Stderr, "* %d objects...", n)
		r, err := benchmarkCacheMemoryN(
			ctx,
			opts,
			createObj,
			fuzzObj,
			n)
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

	fmt.Fprintln(os.Stderr)

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
	createObj objectCreator,
	fuzzObj fuzzer.FuzzerFn,
	n int) (_ benchmarkCacheMemoryResult, retErr error) {

	mgr, err := manager.New(opts.Config, manager.Options{})
	if err != nil {
		return benchmarkCacheMemoryResult{},
			fmt.Errorf("create manager: %w", err)
	}

	gvk := opts.GroupVersionKind

	crciController.SkipNameValidation = ptr(true)
	if err := crciController.AddToManager(mgr, gvk); err != nil {
		return benchmarkCacheMemoryResult{},
			fmt.Errorf("add controller: %w", err)
	}

	mgrCtx, mgrCancel := context.WithCancel(ctx)
	go func() {
		_ = mgr.Start(mgrCtx)
	}()
	defer mgrCancel()

	k8sClient := mgr.GetClient()

	ns := corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "crci-",
		},
	}
	if err := k8sClient.Create(ctx, &ns); err != nil {
		return benchmarkCacheMemoryResult{},
			fmt.Errorf("failed to create namespace: %w", err)
	}
	defer func() {
		if err := k8sClient.Delete(ctx, &ns); err != nil {
			err := fmt.Errorf("failed to delete namespace: %w", err)
			if retErr == nil {
				retErr = err
			} else {
				retErr = errors.Join(retErr, err)
			}
		}
	}()

	if !mgr.GetCache().WaitForCacheSync(ctx) {
		return benchmarkCacheMemoryResult{},
			fmt.Errorf("cache sync wait failed")
	}

	objs := make([]client.Object, n)
	for i := range n {
		objs[i] = createObj()
		fuzzObj(objs[i], ns.Name, i)
	}

	var bytesYAML float64
	for i := range objs {
		data, _ := yaml.Marshal(objs[i])
		bytesYAML += float64(len(data))
	}

	// Check if the object schema has a status field.
	hasStatus := false
	_, ok, err := unstructured.NestedFieldNoCopy(
		objs[0].(*unstructured.Unstructured).Object,
		"status")
	if err != nil {
		return benchmarkCacheMemoryResult{},
			fmt.Errorf("get status: %w", err)
	}
	if ok {
		hasStatus = true
	}

	var objStatuses []any

	if hasStatus {
		objStatuses = make([]any, n)
		for i, obj := range objs {
			status, ok, err := unstructured.NestedFieldCopy(
				obj.(*unstructured.Unstructured).Object, "status")
			if err != nil {
				return benchmarkCacheMemoryResult{},
					fmt.Errorf("get status: %w", err)
			}
			if !ok {
				return benchmarkCacheMemoryResult{},
					fmt.Errorf("status not found")
			}
			objStatuses[i] = status
		}
	}

	// Load N resources with fuzz-style data (unique names, varied labels/data)
	for i := 0; i < len(objs); i++ {
		if err := k8sClient.Create(ctx, objs[i]); err != nil {
			return benchmarkCacheMemoryResult{},
				fmt.Errorf("create resource %d: %w", i, err)
		}
		if hasStatus {
			unstructured.SetNestedField(
				objs[i].(*unstructured.Unstructured).Object,
				objStatuses[i],
				"status")
			if err := k8sClient.Status().Update(ctx, objs[i]); err != nil {
				return benchmarkCacheMemoryResult{},
					fmt.Errorf("update status %d: %w", i, err)
			}
		}
	}

	if !mgr.GetCache().WaitForCacheSync(ctx) {
		return benchmarkCacheMemoryResult{},
			fmt.Errorf("cache sync after create failed")
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
