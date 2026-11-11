// SPDX-FileCopyrightText: Contributors to the Gardener project
//
// SPDX-License-Identifier: Apache-2.0

package extension

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/component-base/version"
	"k8s.io/utils/clock"
	testclock "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1beta1helper "github.com/gardener/gardener/pkg/api/core/v1beta1/helper"
	gardencorev1 "github.com/gardener/gardener/pkg/apis/core/v1"
	gardencorev1beta1 "github.com/gardener/gardener/pkg/apis/core/v1beta1"
	v1beta1constants "github.com/gardener/gardener/pkg/apis/core/v1beta1/constants"
	operatorv1alpha1 "github.com/gardener/gardener/pkg/apis/operator/v1alpha1"
	"github.com/gardener/gardener/pkg/client/kubernetes"
	fakeclientmap "github.com/gardener/gardener/pkg/client/kubernetes/clientmap/fake"
	"github.com/gardener/gardener/pkg/client/kubernetes/clientmap/keys"
	fakekubernetes "github.com/gardener/gardener/pkg/client/kubernetes/fake"
	operatorclient "github.com/gardener/gardener/pkg/operator/client"
	"github.com/gardener/gardener/pkg/operator/controller/extension/extension/admission"
	"github.com/gardener/gardener/pkg/operator/controller/extension/extension/controllerregistration"
	"github.com/gardener/gardener/pkg/operator/controller/extension/extension/runtime"
	"github.com/gardener/gardener/pkg/utils/oci"
	ocifake "github.com/gardener/gardener/pkg/utils/oci/fake"
)

func TestCompatibilityMessages(t *testing.T) {
	for _, tc := range []struct {
		name, running, builtWith, testedWith string
		requires                             string
		messages                             []string
		error                                string
	}{
		{name: "no compatibility metadata", running: "unknown"},
		{name: "within bounds", running: "v1.154.0", builtWith: "v1.153.0", testedWith: "v1.154.1"},
		{name: "equal to built version", running: "v1.153.0", builtWith: "v1.153.0"},
		{name: "equal to tested version", running: "v1.154.1", testedWith: "v1.154.1"},
		{name: "older than built version", running: "v1.152.0", builtWith: "v1.153.0", messages: []string{"older than"}},
		{name: "newer than tested version", running: "v1.155.0", testedWith: "v1.154.1", messages: []string{"newer than"}},
		{name: "both messages", running: "v1.154.0", builtWith: "v1.155.0", testedWith: "v1.153.0", messages: []string{"older than", "newer than"}},
		{name: "numeric comparison", running: "v1.9.0", builtWith: "v1.10.0", messages: []string{"older than"}},
		{name: "prerelease", running: "v1.154.0-dev", builtWith: "v1.154.0", messages: []string{"older than"}},
		{name: "build metadata", running: "v1.154.0+abc", builtWith: "1.154.0", testedWith: "v1.154.0"},
		{name: "invalid built version", running: "v1.154.0", builtWith: "invalid", error: "invalid builtWith gardener/gardener version"},
		{name: "invalid tested version", running: "v1.154.0", testedWith: "invalid", error: "invalid testedWith gardener/gardener version"},
		{name: "invalid running version", running: "unknown", builtWith: "v1.153.0", error: "invalid running Gardener operator version"},
		{name: "requirement satisfied", running: "v1.154.0", requires: ">= 1.135.0"},
		{name: "equal to required minimum", running: "v1.135.0", requires: ">= 1.135.0"},
		{name: "requirement not satisfied", running: "v1.134.0", requires: ">= 1.135.0", messages: []string{"does not satisfy"}},
		{name: "required version range", running: "v1.155.0", requires: ">= 1.135.0, < 1.155.0", messages: []string{"does not satisfy"}},
		{name: "all compatibility messages", running: "v1.154.0", requires: ">= 1.155.0", builtWith: "v1.155.0", testedWith: "v1.153.0", messages: []string{"does not satisfy", "older than", "newer than"}},
		{name: "invalid requirement", running: "v1.154.0", requires: "invalid", error: "invalid requires gardener/gardener constraint"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			m := &extensionCompatibility{}
			if tc.requires != "" {
				m.Requires = map[string]string{"gardener/gardener": tc.requires}
			}
			if tc.builtWith != "" {
				m.BuiltWith = &compatibilityVersions{Gardener: tc.builtWith}
			}
			if tc.testedWith != "" {
				m.TestedWith = &compatibilityVersions{Gardener: tc.testedWith}
			}
			messages, err := m.compatibilityMessages(tc.running)
			if tc.error != "" {
				g.Expect(err).To(MatchError(ContainSubstring(tc.error)))
				return
			}
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(messages).To(HaveLen(len(tc.messages)))
			for i, message := range tc.messages {
				g.Expect(messages[i]).To(ContainSubstring(message))
				g.Expect(messages[i]).To(ContainSubstring(tc.running))
			}
		})
	}
}

func TestReconcileCompatibility(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	extension := &operatorv1alpha1.Extension{
		ObjectMeta: metav1.ObjectMeta{Name: "example", Annotations: map[string]string{operatorv1alpha1.AnnotationExtensionManifest: "ghcr.io/gardener/extensions/example:v0.0.2"}},
		Spec: operatorv1alpha1.ExtensionSpec{
			Resources: []gardencorev1beta1.ControllerResource{{Kind: "Extension", Type: "example"}},
		},
		Status: operatorv1alpha1.ExtensionStatus{
			Conditions: []gardencorev1beta1.Condition{
				{Type: operatorv1alpha1.ExtensionRequiredRuntime, Status: gardencorev1beta1.ConditionTrue},
				{Type: operatorv1alpha1.ExtensionInstalled, Status: gardencorev1beta1.ConditionTrue, Reason: ReasonReconcileSuccess},
			},
		},
	}
	c := fakeclient.NewClientBuilder().WithScheme(operatorclient.RuntimeScheme).WithStatusSubresource(extension).WithObjects(extension).Build()
	registry := ocifake.NewRegistry()
	fakeClock := testclock.NewFakeClock(time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC))
	r := &Reconciler{RuntimeClientSet: fakekubernetes.NewClientSetBuilder().WithClient(c).Build(), CompatibilityRegistry: registry, Clock: fakeClock}
	applyManifest := func(gardenerVersion string) {
		t.Helper()
		conditions := NewConditions(r.Clock, extension.Status)
		conditions.compatible = r.reconcileCompatibility(ctx, extension, gardenerVersion, conditions.compatible)
		g.Expect(r.updateExtensionStatus(ctx, logr.Discard(), extension, conditions)).To(Succeed())
		g.Expect(c.Get(ctx, client.ObjectKeyFromObject(extension), extension)).To(Succeed())
	}
	compatible := func() *gardencorev1beta1.Condition {
		t.Helper()
		condition := v1beta1helper.GetCondition(extension.Status.Conditions, operatorv1alpha1.ExtensionCompatible)
		g.Expect(condition).NotTo(BeNil())
		return condition
	}
	repository := &gardencorev1.OCIRepository{Ref: new(extension.Annotations[operatorv1alpha1.AnnotationExtensionManifest])}
	manifest := `supportedFeatures: [ipv6]
builtWith:
  gardener/gardener: v1.155.0
testedWith:
  gardener/gardener: v1.153.0
  kubernetesVersions: ["1.36", "1.35"]
`
	registry.AddArtifact(repository, []byte(manifest))
	g.Expect(c.Get(ctx, client.ObjectKeyFromObject(extension), extension)).To(Succeed())
	applyManifest("v1.154.0")
	g.Expect(compatible().Status).To(Equal(gardencorev1beta1.ConditionFalse))
	g.Expect(compatible().Reason).To(Equal(ReasonGardenerVersionIncompatible))
	g.Expect(compatible().Message).To(ContainSubstring("older than"))
	g.Expect(compatible().Message).To(ContainSubstring("newer than"))
	g.Expect(extension.Status.Conditions).To(HaveLen(3))
	g.Expect(v1beta1helper.GetCondition(extension.Status.Conditions, operatorv1alpha1.ExtensionRequiredRuntime).Status).To(Equal(gardencorev1beta1.ConditionTrue))
	g.Expect(v1beta1helper.GetCondition(extension.Status.Conditions, operatorv1alpha1.ExtensionInstalled).Status).To(Equal(gardencorev1beta1.ConditionTrue))
	g.Expect(extension.Spec.Resources).To(Equal([]gardencorev1beta1.ControllerResource{{Kind: "Extension", Type: "example"}}))

	// Identical compatibility results must not cause repeated status writes or transitions.
	resourceVersion := extension.ResourceVersion
	transitionTime := compatible().LastTransitionTime
	fakeClock.Step(time.Hour)
	applyManifest("v1.154.0")
	g.Expect(extension.ResourceVersion).To(Equal(resourceVersion))
	g.Expect(compatible().LastTransitionTime).To(Equal(transitionTime))

	// An updated manifest replaces the compatibility message.
	manifest = strings.ReplaceAll(manifest, "v1.155.0", "v1.153.0")
	registry.AddArtifact(repository, []byte(manifest))
	applyManifest("v1.154.0")
	g.Expect(compatible().Status).To(Equal(gardencorev1beta1.ConditionFalse))
	g.Expect(compatible().Message).To(ContainSubstring("newer than"))
	g.Expect(compatible().Message).NotTo(ContainSubstring("older than"))

	manifest = strings.ReplaceAll(manifest, "v1.153.0", "v1.154.0")
	registry.AddArtifact(repository, []byte(manifest))
	applyManifest("v1.154.0")
	g.Expect(compatible().Status).To(Equal(gardencorev1beta1.ConditionTrue))
	g.Expect(compatible().Reason).To(Equal(ReasonGardenerVersionCompatible))
	g.Expect(compatible().Message).To(ContainSubstring("satisfies"))
	g.Expect(compatible().LastTransitionTime).NotTo(Equal(transitionTime))

	// Removing version bounds or the manifest must not leave a stale compatibility result.
	applyManifest("v1.152.0")
	g.Expect(compatible().Status).To(Equal(gardencorev1beta1.ConditionFalse))
	registry.AddArtifact(repository, []byte("name: Example\n"))
	applyManifest("v1.152.0")
	g.Expect(compatible().Status).To(Equal(gardencorev1beta1.ConditionUnknown))
	g.Expect(compatible().Reason).To(Equal(ReasonNoCompatibilityInformation))
	delete(extension.Annotations, operatorv1alpha1.AnnotationExtensionManifest)
	g.Expect(c.Update(ctx, extension)).To(Succeed())
	applyManifest("v1.152.0")
	g.Expect(compatible().Status).To(Equal(gardencorev1beta1.ConditionUnknown))
	g.Expect(compatible().Reason).To(Equal(ReasonNoManifest))
}

func TestCompatibilityDoesNotBlockInstallation(t *testing.T) {
	g := NewWithT(t)
	// Local builds use placeholder build metadata that is not valid semver.
	originalVersion := version.Get().GitVersion
	g.Expect(version.SetDynamicVersion(strings.SplitN(originalVersion, "+", 2)[0])).To(Succeed())
	t.Cleanup(func() { g.Expect(version.SetDynamicVersion(originalVersion)).To(Succeed()) })

	for _, tc := range []struct {
		name, manifest, message string
		registry                oci.Interface
		status                  gardencorev1beta1.ConditionStatus
		reason                  string
		deploymentError         error
	}{
		{name: "missing manifest", message: "failed pulling extension compatibility information"},
		{name: "invalid YAML", manifest: "name: [", message: "failed parsing compatibility.yaml"},
		{name: "invalid built version", manifest: "builtWith:\n  gardener/gardener: invalid\n", message: "failed checking extension compatibility"},
		{name: "invalid tested version", manifest: "testedWith:\n  gardener/gardener: invalid\n", message: "failed checking extension compatibility"},
		{name: "invalid version constraint", manifest: "requires:\n  gardener/gardener: invalid\n", message: "failed checking extension compatibility"},
		{name: "missing compatibility attachment", registry: missingCompatibilityRegistry{}, reason: ReasonNoCompatibilityInformation, message: "No compatibility.yaml artifact"},
		{name: "version mismatch", manifest: "requires:\n  gardener/gardener: '>= 999.0.0'\n", status: gardencorev1beta1.ConditionFalse, reason: ReasonGardenerVersionIncompatible, message: "does not satisfy"},
		{name: "deployment error with missing manifest", message: "failed pulling extension compatibility information", deploymentError: errors.New("deployment failed")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			ctx := context.Background()
			extension := &operatorv1alpha1.Extension{
				ObjectMeta: metav1.ObjectMeta{Name: "example", Generation: 1, Annotations: map[string]string{operatorv1alpha1.AnnotationExtensionManifest: "ghcr.io/gardener/extensions/example:v0.0.2"}},
				Status: operatorv1alpha1.ExtensionStatus{Conditions: []gardencorev1beta1.Condition{
					{Type: operatorv1alpha1.ExtensionRequiredRuntime, Status: gardencorev1beta1.ConditionTrue},
				}},
			}
			garden := &operatorv1alpha1.Garden{
				ObjectMeta: metav1.ObjectMeta{Name: "garden", Annotations: map[string]string{v1beta1constants.AnnotationKeyGenericTokenKubeconfigSecretName: "generic-token-kubeconfig"}},
				Status: operatorv1alpha1.GardenStatus{
					Gardener: &gardencorev1beta1.Gardener{Version: version.Get().GitVersion},
					LastOperation: &gardencorev1beta1.LastOperation{
						Type: gardencorev1beta1.LastOperationTypeReconcile, State: gardencorev1beta1.LastOperationStateSucceeded, Progress: 100,
					},
				},
			}
			c := fakeclient.NewClientBuilder().WithScheme(operatorclient.RuntimeScheme).WithStatusSubresource(extension).WithObjects(extension, garden).Build()
			registry := ocifake.NewRegistry()
			if tc.manifest != "" {
				registry.AddArtifact(&gardencorev1.OCIRepository{Ref: new(extension.Annotations[operatorv1alpha1.AnnotationExtensionManifest])}, []byte(tc.manifest))
			}
			clientSet := fakekubernetes.NewClientSetBuilder().WithClient(c).Build()
			runtimeDeployer := &compatibilityTestRuntime{err: tc.deploymentError}
			admissionDeployer := &compatibilityTestAdmission{}
			registrationDeployer := &compatibilityTestRegistration{}
			r := &Reconciler{
				RuntimeClientSet:      clientSet,
				GardenClientMap:       fakeclientmap.NewClientMapBuilder().WithClientSetForKey(keys.ForGarden(garden), clientSet).Build(),
				CompatibilityRegistry: registry, Clock: clock.RealClock{},
				runtime: runtimeDeployer, admission: admissionDeployer, controllerRegistration: registrationDeployer,
			}
			if tc.registry != nil {
				r.CompatibilityRegistry = tc.registry
			}
			result, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(extension)})
			g.Expect(runtimeDeployer.reconciled).To(BeTrue())
			if tc.deploymentError == nil {
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(result.RequeueAfter).To(Equal(CompatibilityCheckInterval))
				g.Expect(admissionDeployer.reconciled).To(BeTrue())
				g.Expect(registrationDeployer.reconciled).To(BeTrue())
			} else {
				g.Expect(err).To(MatchError(ContainSubstring(tc.deploymentError.Error())))
			}
			g.Expect(c.Get(ctx, client.ObjectKeyFromObject(extension), extension)).To(Succeed())
			installed := v1beta1helper.GetCondition(extension.Status.Conditions, operatorv1alpha1.ExtensionInstalled)
			g.Expect(installed).NotTo(BeNil())
			if tc.deploymentError == nil {
				g.Expect(installed.Status).To(Equal(gardencorev1beta1.ConditionTrue))
				g.Expect(installed.Reason).To(Equal(ReasonReconcileSuccess))
			} else {
				g.Expect(installed.Status).To(Equal(gardencorev1beta1.ConditionFalse))
				g.Expect(installed.Reason).To(Equal(ReasonReconcileFailed))
				g.Expect(installed.Message).To(ContainSubstring(tc.deploymentError.Error()))
			}
			compatible := v1beta1helper.GetCondition(extension.Status.Conditions, operatorv1alpha1.ExtensionCompatible)
			g.Expect(compatible).NotTo(BeNil())
			status, reason := tc.status, tc.reason
			if status == "" {
				status = gardencorev1beta1.ConditionUnknown
			}
			if reason == "" {
				reason = ReasonCompatibilityCheckFailed
			}
			g.Expect(compatible.Status).To(Equal(status))
			g.Expect(compatible.Reason).To(Equal(reason))
			g.Expect(compatible.Message).To(ContainSubstring(tc.message))
			g.Expect(extension.Status.ObservedGeneration).To(Equal(int64(1)))
		})
	}
}

type compatibilityTestRuntime struct {
	runtime.Interface
	reconciled bool
	err        error
}

func (d *compatibilityTestRuntime) Reconcile(context.Context, logr.Logger, *operatorv1alpha1.Extension) (reconcile.Result, error) {
	d.reconciled = true
	return reconcile.Result{}, d.err
}

type compatibilityTestAdmission struct {
	admission.Interface
	reconciled bool
}

func (d *compatibilityTestAdmission) Reconcile(context.Context, logr.Logger, kubernetes.Interface, string, *operatorv1alpha1.Extension) error {
	d.reconciled = true
	return nil
}

type compatibilityTestRegistration struct {
	controllerregistration.Interface
	reconciled bool
}

func (d *compatibilityTestRegistration) Reconcile(context.Context, logr.Logger, *operatorv1alpha1.Extension) error {
	d.reconciled = true
	return nil
}

func TestCompatibilityUpdatedAfterGardenDeletion(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	extension := &operatorv1alpha1.Extension{
		ObjectMeta: metav1.ObjectMeta{Name: "example"},
		Status: operatorv1alpha1.ExtensionStatus{Conditions: []gardencorev1beta1.Condition{
			{Type: operatorv1alpha1.ExtensionInstalled, Status: gardencorev1beta1.ConditionFalse, Reason: ReasonDeleteSuccessful},
			{Type: operatorv1alpha1.ExtensionCompatible, Status: gardencorev1beta1.ConditionFalse, Reason: ReasonGardenerVersionIncompatible},
		}},
	}
	c := fakeclient.NewClientBuilder().WithScheme(operatorclient.RuntimeScheme).WithStatusSubresource(extension).WithObjects(extension).Build()
	r := &Reconciler{RuntimeClientSet: fakekubernetes.NewClientSetBuilder().WithClient(c).Build(), Clock: clock.RealClock{}}
	result, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(extension)})
	g.Expect(err).NotTo(HaveOccurred())
	// This extension has no annotation, so no periodic compatibility check is needed.
	g.Expect(result.RequeueAfter).To(BeZero())
	g.Expect(c.Get(ctx, client.ObjectKeyFromObject(extension), extension)).To(Succeed())
	g.Expect(v1beta1helper.GetCondition(extension.Status.Conditions, operatorv1alpha1.ExtensionInstalled).Reason).To(Equal(ReasonDeleteSuccessful))
	compatible := v1beta1helper.GetCondition(extension.Status.Conditions, operatorv1alpha1.ExtensionCompatible)
	g.Expect(compatible.Status).To(Equal(gardencorev1beta1.ConditionUnknown))
	g.Expect(compatible.Reason).To(Equal(ReasonNoManifest))
}

func TestWithoutCompatibilityInformation(t *testing.T) {
	g := NewWithT(t)
	ctx := context.Background()
	extension := &operatorv1alpha1.Extension{ObjectMeta: metav1.ObjectMeta{Name: "example", Annotations: map[string]string{operatorv1alpha1.AnnotationExtensionManifest: "example.com/extension:v1"}}}
	c := fakeclient.NewClientBuilder().WithScheme(operatorclient.RuntimeScheme).WithStatusSubresource(extension).WithObjects(extension).Build()
	registry := ocifake.NewRegistry()
	registry.AddArtifact(&gardencorev1.OCIRepository{Ref: new(extension.Annotations[operatorv1alpha1.AnnotationExtensionManifest])}, []byte("name: Example\ntestedWith:\n  kubernetesVersions: [\"1.36\"]\n"))
	r := &Reconciler{RuntimeClientSet: fakekubernetes.NewClientSetBuilder().WithClient(c).Build(), CompatibilityRegistry: registry, Clock: clock.RealClock{}}
	result, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(extension)})
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(result.RequeueAfter).To(Equal(CompatibilityCheckInterval))
	g.Expect(c.Get(ctx, client.ObjectKeyFromObject(extension), extension)).To(Succeed())
	compatible := v1beta1helper.GetCondition(extension.Status.Conditions, operatorv1alpha1.ExtensionCompatible)
	g.Expect(compatible).NotTo(BeNil())
	g.Expect(compatible.Status).To(Equal(gardencorev1beta1.ConditionUnknown))
	g.Expect(compatible.Reason).To(Equal(ReasonNoCompatibilityInformation))
	g.Expect(v1beta1helper.GetCondition(extension.Status.Conditions, operatorv1alpha1.ExtensionInstalled).Reason).To(Equal(ReasonNoGardenFound))
	// The compatibility attachment is optional, including when the extension artifact exists without attachments.
	r.CompatibilityRegistry = missingCompatibilityRegistry{}
	_, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(extension)})
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(c.Get(ctx, client.ObjectKeyFromObject(extension), extension)).To(Succeed())
	compatible = v1beta1helper.GetCondition(extension.Status.Conditions, operatorv1alpha1.ExtensionCompatible)
	g.Expect(compatible.Status).To(Equal(gardencorev1beta1.ConditionUnknown))
	g.Expect(compatible.Reason).To(Equal(ReasonNoCompatibilityInformation))
	g.Expect(compatible.Message).To(ContainSubstring("No compatibility.yaml artifact"))
	// Omitting the field never requires a registry.
	delete(extension.Annotations, operatorv1alpha1.AnnotationExtensionManifest)
	g.Expect(c.Update(ctx, extension)).To(Succeed())
	r.CompatibilityRegistry = nil
	result, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(extension)})
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(result.RequeueAfter).To(BeZero())
	g.Expect(c.Get(ctx, client.ObjectKeyFromObject(extension), extension)).To(Succeed())
	compatible = v1beta1helper.GetCondition(extension.Status.Conditions, operatorv1alpha1.ExtensionCompatible)
	g.Expect(compatible.Status).To(Equal(gardencorev1beta1.ConditionUnknown))
	g.Expect(compatible.Reason).To(Equal(ReasonNoManifest))
}

type missingCompatibilityRegistry struct{}

func (missingCompatibilityRegistry) Pull(context.Context, *gardencorev1.OCIRepository) ([]byte, error) {
	return nil, oci.ErrNoCompatibilityArtifact
}
