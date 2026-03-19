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

	"github.com/go-logr/logr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/akutz/crci/benchmark"
)

var (
	resourceFlag     string
	crdFilePathsFlag string
	crdDirPathsFlag  string
	outputFormatFlag string
	dryRunFlag       bool
	fibonacciFlag    int
	pauseFlag        bool
	printYAMLFlag    bool
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
		&crdFilePathsFlag,
		"crds",
		"",
		"Comma-separated paths to CRD YAML files to "+
			"load (e.g. ./crds/vm.yaml,./crds/other.yaml)")

	flag.StringVar(
		&crdDirPathsFlag,
		"crd-dirs",
		"",
		"Comma-separated paths to directories with CRD YAML files to "+
			"load (e.g. ./,./crds/)")

	flag.StringVar(
		&outputFormatFlag,
		"output-format",
		"text",
		"The output format: text, markdown, csv. Defaults to text.")

	flag.IntVar(
		&fibonacciFlag,
		"fib",
		14, // 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610
		"The number of fibonacci numbers to use."+
			"Defaults to 14 (which is 610).")

	flag.BoolVar(
		&dryRunFlag,
		"dry-run",
		false,
		"Set to true to print the benchmark summary and exit.")

	flag.BoolVar(
		&pauseFlag,
		"pause",
		false,
		"Set to true to pause the program before running the benchmark. "+
			"Useful for debugging envtest using kubectl.")

	flag.BoolVar(
		&printYAMLFlag,
		"print-yaml",
		false,
		"Set to true to print the YAML of one of the generated objects.")

	flag.Parse()

	if fibonacciFlag < 2 {
		abortWithUsage("Invalid argument: -fib must be >= 2\n")
	}

	outputFormat := benchmark.OutputFormat(outputFormatFlag)
	switch outputFormat {
	case benchmark.OutputFormatCSV,
		benchmark.OutputFormatMarkdown,
		benchmark.OutputFormatText:
		// Allowed
	default:
		abortWithUsage(
			"Invalid argument: -format must be one of: %s, %s, %s\n",
			benchmark.OutputFormatCSV,
			benchmark.OutputFormatMarkdown,
			benchmark.OutputFormatText)
	}

	ctrl.SetLogger(logr.Discard())

	opts := benchmark.BenchmarkCacheMemoryOptions{
		DryRun:       dryRunFlag,
		OutputFormat: outputFormat,
		Fibonacci:    fibonacciFlag,
		Pause:        pauseFlag,
		PrintYAML:    printYAMLFlag,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		cleanupFns  []func() error
		cleanup     func()
		cleanupOnce sync.Once
	)

	cleanup = func() {
		cleanupOnce.Do(func() {
			cancel()
			fmt.Fprintln(os.Stderr)
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
		cleanup()
	}()

	var (
		env        envtest.Environment
		kubeconfig string
	)

	// Determine whether to use an existing cluster or start a new one.
	if f := flag.Lookup("kubeconfig"); f != nil && f.Value.String() != "" {
		kubeconfig = f.Value.String()
	} else {
		kubeconfig = os.Getenv("KUBECONFIG")
	}
	if kubeconfig == "" {
		kubeconfig = ".kubeconfig"
	}
	if _, err := os.Stat(kubeconfig); err != nil {
		if !os.IsNotExist(err) {
			abort("failed to stat kubeconfig file: %v\n", err)
		}
		env.UseExistingCluster = &[]bool{false}[0]
	} else {
		env.UseExistingCluster = &[]bool{true}[0]
	}
	opts.KubeConfig = kubeconfig

	// If not using an existing cluster, configure the envtest.
	if env.UseExistingCluster != nil && !*env.UseExistingCluster {

		// Customize the API server's arguments.
		apiServer := env.ControlPlane.GetAPIServer()
		args := apiServer.Configure()
		args.Set(
			"disable-admission-plugins",
			strings.Join(defaultAdmissionPlugins, ","))

		// Install the CRDs.
		if v := crdFilePathsFlag; v != "" {
			var paths []string
			for _, p := range strings.Split(v, ",") {
				if s := strings.TrimSpace(p); s != "" {
					paths = append(paths, s)
				}
			}
			env.CRDInstallOptions.Paths = paths
			env.CRDInstallOptions.ErrorIfPathMissing = true
		}
		if v := crdDirPathsFlag; v != "" {
			var paths []string
			for _, p := range strings.Split(v, ",") {
				if s := strings.TrimSpace(p); s != "" {
					paths = append(paths, s)
				}
			}
			env.CRDDirectoryPaths = paths
			env.CRDInstallOptions.ErrorIfPathMissing = true
		}
	}

	// Start the envtest.
	cfg, err := env.Start()
	if err != nil {
		abort("failed to start envtest: %v\n", err)
	}
	cleanupFns = append(cleanupFns, func() error {
		fmt.Fprintln(os.Stderr, "* Stopping envtest")
		if err := env.Stop(); err != nil {
			return fmt.Errorf("failed to stop envtest: %w", err)
		}
		return nil
	})
	opts.Config = cfg

	// If not using an existing cluster, emit the envtest kubeconfig to the
	// file specified by the -kubeconfig flag.
	if env.UseExistingCluster != nil && !*env.UseExistingCluster {
		// Ensure the kubeconfig file is deleted when the benchmark is
		// complete or the program is interrupted if kubeconfig does not
		// point to an existing cluster.
		cleanupFns = append(cleanupFns, func() error {
			fmt.Fprintf(os.Stderr, "* Removing %q\n", opts.KubeConfig)
			_ = os.RemoveAll(opts.KubeConfig)
			return nil
		})

		// Create the kubeconfig file.
		f, err := os.Create(opts.KubeConfig)
		if err != nil {
			abort("failed to create temp kubeconfig file: %v\n", err)
		}
		if _, err := io.Copy(f, bytes.NewReader(env.KubeConfig)); err != nil {
			abort("failed to write kubeconfig to temp file: %v\n", err)
		}
	}

	// Get the GroupVersionKind for the resource to test.
	gvk, err := benchmark.ResolveGroupVersionKind(
		strings.TrimSpace(resourceFlag))
	if err != nil {
		abort("failed to resolve gvk for %q: %v\n", resourceFlag, err)
	}
	opts.GroupVersionKind = gvk

	// Run the benchmark.
	if err := benchmark.BenchmarkCacheMemory(ctx, opts); err != nil {
		abort("benchmark failed: %v\n", err)
	}
}

func abortWithUsage(format string, a ...any) {
	fmt.Fprintf(os.Stderr, format, a...)
	flag.Usage()
	os.Exit(1)
}

// From https://kubernetes.io/docs/reference/command-line-tools-reference/kube-apiserver/
var defaultAdmissionPlugins = []string{
	"NamespaceLifecycle",
	"LimitRanger",
	"ServiceAccount",
	"TaintNodesByCondition",
	"PodSecurity",
	"Priority",
	"DefaultTolerationSeconds",
	"DefaultStorageClass",
	"StorageObjectInUseProtection",
	"PersistentVolumeClaimResize",
	"RuntimeClass",
	"CertificateApproval",
	"CertificateSigning",
	"ClusterTrustBundleAttest",
	"CertificateSubjectRestriction",
	"DefaultIngressClass",
	"PodTopologyLabels",
	"NodeDeclaredFeatureValidator",
	"MutatingAdmissionPolicy",
	"MutatingAdmissionWebhook",
	"ValidatingAdmissionPolicy",
	"ValidatingAdmissionWebhook",
	"ResourceQuota",
}
