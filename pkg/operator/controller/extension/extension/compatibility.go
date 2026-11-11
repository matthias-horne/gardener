// SPDX-FileCopyrightText: Contributors to the Gardener project
//
// SPDX-License-Identifier: Apache-2.0

package extension

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/Masterminds/semver/v3"
	"sigs.k8s.io/yaml"

	v1beta1helper "github.com/gardener/gardener/pkg/api/core/v1beta1/helper"
	gardencorev1 "github.com/gardener/gardener/pkg/apis/core/v1"
	gardencorev1beta1 "github.com/gardener/gardener/pkg/apis/core/v1beta1"
	operatorv1alpha1 "github.com/gardener/gardener/pkg/apis/operator/v1alpha1"
	"github.com/gardener/gardener/pkg/utils/oci"
)

const (
	// ReasonNoManifest indicates that no extension manifest annotation is specified.
	ReasonNoManifest = "NoManifest"
	// ReasonNoCompatibilityInformation indicates that no attached Gardener version bounds are available.
	ReasonNoCompatibilityInformation = "NoCompatibilityInformation"
	// ReasonCompatibilityCheckFailed indicates that the compatibility check could not be completed.
	ReasonCompatibilityCheckFailed = "CompatibilityCheckFailed"
	// ReasonGardenerVersionCompatible indicates that the operator version satisfies the manifest's version bounds.
	ReasonGardenerVersionCompatible = "GardenerVersionCompatible"
	// ReasonGardenerVersionIncompatible indicates that the operator version is outside the manifest's version bounds.
	ReasonGardenerVersionIncompatible = "GardenerVersionIncompatible"
)

type extensionCompatibility struct {
	Requires          map[string]string      `json:"requires"`
	SupportedFeatures []string               `json:"supportedFeatures"`
	BuiltWith         *compatibilityVersions `json:"builtWith"`
	TestedWith        *compatibilityVersions `json:"testedWith"`
}

type compatibilityVersions struct {
	Gardener           string   `json:"gardener/gardener"`
	KubernetesVersions []string `json:"kubernetesVersions"`
}

// reconcileCompatibility reports compatibility information without affecting extension installation.
func (r *Reconciler) reconcileCompatibility(ctx context.Context, extension *operatorv1alpha1.Extension, gardenerVersion string, condition gardencorev1beta1.Condition) gardencorev1beta1.Condition {
	updated := func(status gardencorev1beta1.ConditionStatus, reason, message string) gardencorev1beta1.Condition {
		return v1beta1helper.UpdatedConditionWithClock(r.Clock, condition, status, reason, message)
	}
	failed := func(err error) gardencorev1beta1.Condition {
		return updated(gardencorev1beta1.ConditionUnknown, ReasonCompatibilityCheckFailed, err.Error())
	}
	ref, present := extension.Annotations[operatorv1alpha1.AnnotationExtensionManifest]
	if !present {
		return updated(gardencorev1beta1.ConditionUnknown, ReasonNoManifest, "No extension manifest annotation is specified; Gardener version compatibility cannot be determined")
	}

	data, err := r.CompatibilityRegistry.Pull(ctx, &gardencorev1.OCIRepository{Ref: &ref})
	if errors.Is(err, oci.ErrNoCompatibilityArtifact) {
		return updated(gardencorev1beta1.ConditionUnknown, ReasonNoCompatibilityInformation, "No compatibility.yaml artifact is attached to the extension manifest")
	}
	if err != nil {
		return failed(fmt.Errorf("failed pulling extension compatibility information from OCI repository %q: %w", ref, err))
	}

	manifest := &extensionCompatibility{}
	if err := yaml.Unmarshal(data, manifest); err != nil {
		return failed(fmt.Errorf("failed parsing compatibility.yaml attached to OCI repository %q: %w", ref, err))
	}
	if !manifest.hasGardenerVersionInformation() {
		return updated(gardencorev1beta1.ConditionUnknown, ReasonNoCompatibilityInformation, "The attached compatibility.yaml does not specify Gardener version compatibility information")
	}

	messages, err := manifest.compatibilityMessages(gardenerVersion)
	if err != nil {
		return failed(fmt.Errorf("failed checking extension compatibility: %w", err))
	}
	if len(messages) > 0 {
		return updated(gardencorev1beta1.ConditionFalse, ReasonGardenerVersionIncompatible, strings.Join(messages, "; "))
	}
	return updated(gardencorev1beta1.ConditionTrue, ReasonGardenerVersionCompatible, fmt.Sprintf("Running Gardener operator version %s satisfies the Gardener version compatibility bounds specified in compatibility.yaml", gardenerVersion))
}

func (m *extensionCompatibility) hasGardenerVersionInformation() bool {
	return m.Requires["gardener/gardener"] != "" || (m.BuiltWith != nil && m.BuiltWith.Gardener != "") || (m.TestedWith != nil && m.TestedWith.Gardener != "")
}

func (m *extensionCompatibility) compatibilityMessages(gardenerVersion string) ([]string, error) {
	var messages []string
	if !m.hasGardenerVersionInformation() {
		return nil, nil
	}
	running, err := semver.NewVersion(gardenerVersion)
	if err != nil {
		return nil, fmt.Errorf("invalid running Gardener operator version %q: %w", gardenerVersion, err)
	}
	if requirement := m.Requires["gardener/gardener"]; requirement != "" {
		constraint, err := semver.NewConstraint(requirement)
		if err != nil {
			return nil, fmt.Errorf("invalid requires gardener/gardener constraint %q: %w", requirement, err)
		}
		if !constraint.Check(running) {
			messages = append(messages, fmt.Sprintf("Running Gardener operator version %s does not satisfy the required Gardener version constraint %q", gardenerVersion, requirement))
		}
	}
	for _, check := range []struct {
		field         string
		compatibility *compatibilityVersions
		compare       func(*semver.Version, *semver.Version) bool
		message       string
	}{
		{"builtWith", m.BuiltWith, (*semver.Version).LessThan, "Running Gardener operator version %s is older than the Gardener version %s the extension was built with"},
		{"testedWith", m.TestedWith, (*semver.Version).GreaterThan, "Running Gardener operator version %s is newer than the Gardener version %s the extension was tested with"},
	} {
		if check.compatibility == nil || check.compatibility.Gardener == "" {
			continue
		}
		bound, err := semver.NewVersion(check.compatibility.Gardener)
		if err != nil {
			return nil, fmt.Errorf("invalid %s gardener/gardener version %q: %w", check.field, check.compatibility.Gardener, err)
		}
		if check.compare(running, bound) {
			messages = append(messages, fmt.Sprintf(check.message, gardenerVersion, check.compatibility.Gardener))
		}
	}
	return messages, nil
}
