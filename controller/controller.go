package controller

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

// SkipNameValidation may be set to true in tests/benchmarks to allow multiple
// controllers with the same logical name (e.g. when starting multiple envtest runs).
var SkipNameValidation *bool

// AddToManager adds the watch controller to the manager for the given watched resource type.
func AddToManager(mgr manager.Manager, gvk schema.GroupVersionKind) error {
	obj, err := objectForGVK(mgr.GetScheme(), gvk)
	if err != nil {
		return fmt.Errorf("object for %s: %w", gvk, err)
	}

	r := &Reconciler{
		Client: mgr.GetClient(),
		Logger: ctrl.Log.WithName("controllers").WithName("watch"),
	}

	opts := controller.Options{}
	if SkipNameValidation != nil && *SkipNameValidation {
		opts.SkipNameValidation = SkipNameValidation
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(obj).
		WithOptions(opts).
		Complete(r)
}

// Reconciler reconciles the watched resource type (no-op; used to drive the cache).
type Reconciler struct {
	client.Client
	Logger logr.Logger
}

// Reconcile is a no-op; the controller exists to populate and hold the cache.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	_ = req
	r.Logger.V(2).Info("reconcile", "request", req)
	return ctrl.Result{}, nil
}

// objectForGVK returns a new empty object for the given GVK so the controller
// can use For(obj). It uses Unstructured so any API type can be watched,
// including non-core CRDs that have no concrete Go type registered in the
// scheme.
func objectForGVK(
	_ *runtime.Scheme,
	gvk schema.GroupVersionKind) (client.Object, error) {

	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(gvk)
	return u, nil
}
