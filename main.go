package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/go-logr/logr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/akutz/crci/benchmark"
)

var (
	resourceFlag       string
	crdsFlag           string
	outputFormatFlag   string
	fibonacciFlag      int
	printFibonacciFlag bool
	pauseFlag          bool
)

func main() {

	flag.StringVar(
		&resourceFlag,
		"resource",
		"configmap",
		"Kubernetes resource to test: configmap, secret, pod, "+
			"or group/version/kind",
	)

	flag.StringVar(
		&crdsFlag,
		"crds",
		"",
		"Comma-separated paths to CRD YAML files to load into envtest "+
			"(e.g. crds/vm.yaml,crds/other.yaml)")

	flag.StringVar(
		&outputFormatFlag,
		"format",
		"text",
		"The output format: text, markdown, csv. Defaults to text.")

	flag.IntVar(
		&fibonacciFlag,
		"fib",
		14, // 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610
		"The number of fibonacci numbers to use. Defaults to 14 (which is 610).")

	flag.BoolVar(
		&printFibonacciFlag,
		"fib-print",
		false,
		"Set to true to print the fibonacci sequence to the console.")

	flag.BoolVar(
		&pauseFlag,
		"pause",
		false,
		"Set to true to pause the program after the API server is started.")

	flag.Parse()

	if fibonacciFlag < 2 {
		fmt.Fprintf(os.Stderr, "Invalid argument: -fib must be >= 2\n")
		flag.Usage()
		os.Exit(1)
	}

	outputFormat := benchmark.OutputFormat(outputFormatFlag)
	switch outputFormat {
	case benchmark.OutputFormatCSV,
		benchmark.OutputFormatMarkdown,
		benchmark.OutputFormatText:
		// Allowed
	default:
		fmt.Fprintf(
			os.Stderr,
			"Invalid argument: -format must be one of: %s, %s, %s\n",
			benchmark.OutputFormatCSV,
			benchmark.OutputFormatMarkdown,
			benchmark.OutputFormatText)
		flag.Usage()
		os.Exit(1)
	}

	ctrl.SetLogger(logr.Discard())

	opts := benchmark.BenchmarkCacheMemoryOptions{
		OutputFormat:   outputFormat,
		Fibonacci:      fibonacciFlag,
		FibonacciPrint: printFibonacciFlag,
		Pause:          pauseFlag,
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Minute)
	defer cancel()

	var (
		cleanupFns  []func() error
		cleanup     func()
		cleanupOnce sync.Once
	)

	cleanup = func() {
		cleanupOnce.Do(func() {
			for _, fn := range cleanupFns {
				if err := fn(); err != nil {
					fmt.Fprintf(os.Stderr, "cleanup failed: %v\n", err)
				}
			}
		})
	}

	defer cleanup()

	abort := func(format string, a ...any) {
		cleanup()
		fmt.Fprintf(os.Stderr, format, a...)
		os.Exit(1)
	}

	// Ensure the envtest is stopped if the process is interrupted. Otherwise
	// the etcd and kube-apiserver processes started by envtest will remain
	// running.
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs,
		syscall.SIGINT,
		syscall.SIGABRT,
		syscall.SIGQUIT,
		syscall.SIGTERM)
	go func() {
		<-sigs
		cancel()
		cleanup()
	}()

	var (
		env                  envtest.Environment
		externalControlPlane bool
		kubeconfig           string
	)

	if f := flag.Lookup("kubeconfig"); f != nil && f.Value.String() != "" {
		kubeconfig = f.Value.String()
	} else {
		kubeconfig = os.Getenv("KUBECONFIG")
	}
	if kubeconfig == "" {
		kubeconfig = ".kubeconfig"
	}

	opts.KubeConfig = kubeconfig

	if _, err := os.Stat(opts.KubeConfig); err != nil {
		if !os.IsNotExist(err) {
			abort("failed to stat kubeconfig file: %v\n", err)
		}
	} else {
		env.UseExistingCluster = &[]bool{true}[0]
		externalControlPlane = true
	}

	if !externalControlPlane && crdsFlag != "" {
		var crds []string
		for _, p := range strings.Split(crdsFlag, ",") {
			if s := strings.TrimSpace(p); s != "" {
				crds = append(crds, s)
			}
		}
		env.CRDInstallOptions.Paths = crds
		env.CRDInstallOptions.ErrorIfPathMissing = true
	}

	cfg, err := env.Start()
	if err != nil {
		abort("envtest start failed: %v\n", err)
	}
	cleanupFns = append(cleanupFns, func() error {
		if err := env.Stop(); err != nil {
			return fmt.Errorf("envtest stop failed: %w", err)
		}
		return nil
	})
	opts.Config = cfg

	if !externalControlPlane {
		f, err := os.Create(opts.KubeConfig)
		if err != nil {
			abort("failed to create temp kubeconfig file: %v\n", err)
		}
		cleanupFns = append(cleanupFns, func() error {
			_ = os.RemoveAll(f.Name())
			return nil
		})
		if _, err := io.Copy(f, bytes.NewReader(env.KubeConfig)); err != nil {
			abort("failed to write kubeconfig to temp file: %v\n", err)
		}
	}

	gvk, err := benchmark.ResolveGroupVersionKind(
		strings.TrimSpace(resourceFlag))
	if err != nil {
		abort("failed to resolve gvk for %q: %v\n", resourceFlag, err)
	}
	opts.GroupVersionKind = gvk

	if err := benchmark.BenchmarkCacheMemory(ctx, opts); err != nil {
		abort("benchmark failed: %v\n", err)
	}
}
