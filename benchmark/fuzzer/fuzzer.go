package fuzzer

import (
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// FuzzerFn is a function that fuzzes the provided object.
type FuzzerFn func(obj client.Object, namespace string, i int) error
