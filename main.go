package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/go-logr/logr"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/akutz/crci/benchmark"
)

var (
	resourceFlag       string
	crdsFlag           string
	outputFormatFlag   string
	fibonacciFlag      int
	printFibonacciFlag bool
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
		ResourceName:   strings.TrimSpace(resourceFlag),
		OutputFormat:   outputFormat,
		Fibonacci:      fibonacciFlag,
		FibonacciPrint: printFibonacciFlag,
	}

	if crdsFlag != "" {
		for _, p := range strings.Split(crdsFlag, ",") {
			if s := strings.TrimSpace(p); s != "" {
				opts.CRDs = append(opts.CRDs, s)
			}
		}
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Minute)
	defer cancel()

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
	}()

	if err := benchmark.BenchmarkCacheMemory(ctx, opts); err != nil {
		fmt.Fprintf(os.Stderr, "benchmark failed: %v\n", err)
		os.Exit(1)
	}
}
