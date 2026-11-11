// SPDX-FileCopyrightText: Contributors to the Gardener project
//
// SPDX-License-Identifier: Apache-2.0

package oci

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	gcrv1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"sigs.k8s.io/controller-runtime/pkg/client"

	gardencorev1 "github.com/gardener/gardener/pkg/apis/core/v1"
)

const mediaTypeCompatibility = "application/gardener.compatibility+yaml"

// ErrNoCompatibilityArtifact indicates that no compatibility artifact is attached to the extension artifact.
var ErrNoCompatibilityArtifact = errors.New("no compatibility artifact attached to the extension artifact")

// CompatibilityRegistry discovers and pulls compatibility.yaml artifacts attached to published extension.yaml artifacts.
type CompatibilityRegistry struct {
	registry *HelmRegistry
}

// NewCompatibilityRegistry creates a CompatibilityRegistry. The client is used to retrieve registry secrets if needed.
func NewCompatibilityRegistry(c client.Client) *CompatibilityRegistry {
	return &CompatibilityRegistry{registry: NewHelmRegistry(c)}
}

// Pull returns compatibility.yaml from the newest compatibility artifact attached to the referenced extension.
// Referrers are rediscovered on each pull, including via the OCI referrers tag fallback used by GHCR.
func (r *CompatibilityRegistry) Pull(ctx context.Context, repository *gardencorev1.OCIRepository) ([]byte, error) {
	ref, err := buildRef(repository)
	if err != nil {
		return nil, err
	}
	opts, err := r.registry.remoteOptions(ctx, repository)
	if err != nil {
		return nil, err
	}
	subject, err := remote.Get(ref, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed resolving extension artifact %s: %w", ref, err)
	}
	digestRef := ref.Context().Digest(subject.Digest.String())
	referrerOpts := append([]remote.Option{}, opts...)
	referrerOpts = append(referrerOpts, remote.WithFilter("artifactType", mediaTypeCompatibility), remote.WithReferrersTagFallback(true))
	index, err := remote.Referrers(digestRef, referrerOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed discovering compatibility artifacts for %s: %w", digestRef, err)
	}
	manifest, err := index.IndexManifest()
	if err != nil {
		return nil, fmt.Errorf("failed parsing compatibility referrers: %w", err)
	}
	// Some registries omit artifact annotations from referrer descriptors. Read them from the artifacts when choosing between attachments.
	if len(manifest.Manifests) > 1 {
		for i := range manifest.Manifests {
			descriptor := &manifest.Manifests[i]
			if descriptor.Annotations["org.opencontainers.image.created"] != "" {
				continue
			}
			image, err := remote.Image(ref.Context().Digest(descriptor.Digest.String()), opts...)
			if err != nil {
				return nil, fmt.Errorf("failed fetching compatibility artifact metadata: %w", err)
			}
			artifactManifest, err := image.Manifest()
			if err != nil {
				return nil, fmt.Errorf("failed parsing compatibility artifact metadata: %w", err)
			}
			descriptor.Annotations = artifactManifest.Annotations
		}
	}
	attachment, err := newestCompatibilityArtifact(manifest.Manifests)
	if err != nil {
		return nil, err
	}
	attachmentRepository := repository.DeepCopy()
	attachmentRepository.Ref = new(ref.Context().Digest(attachment.Digest.String()).Name())
	attachmentRepository.Repository, attachmentRepository.Tag, attachmentRepository.Digest = nil, nil, nil
	// Cache by the attachment digest, so new test results on the same extension digest are picked up.
	return r.registry.pull(ctx, attachmentRepository, "compatibility:", extractCompatibilityLayer)
}

func newestCompatibilityArtifact(descriptors []gcrv1.Descriptor) (gcrv1.Descriptor, error) {
	var selected gcrv1.Descriptor
	var selectedTime time.Time
	for _, descriptor := range descriptors {
		if descriptor.ArtifactType != mediaTypeCompatibility {
			continue
		}
		var created time.Time
		if value := descriptor.Annotations["org.opencontainers.image.created"]; value != "" {
			var err error
			created, err = time.Parse(time.RFC3339Nano, value)
			if err != nil {
				return gcrv1.Descriptor{}, fmt.Errorf("invalid compatibility artifact creation time %q: %w", value, err)
			}
		}
		if selected.Digest.Hex == "" || created.After(selectedTime) || (created.Equal(selectedTime) && descriptor.Digest.String() > selected.Digest.String()) {
			selected, selectedTime = descriptor, created
		}
	}
	if selected.Digest.Hex == "" {
		return gcrv1.Descriptor{}, ErrNoCompatibilityArtifact
	}
	return selected, nil
}

func extractCompatibilityLayer(image gcrv1.Image) ([]byte, error) {
	manifest, err := image.Manifest()
	if err != nil {
		return nil, fmt.Errorf("failed to parse OCI manifest: %w", err)
	}

	for _, descriptor := range manifest.Layers {
		if descriptor.Annotations["org.opencontainers.image.title"] != "compatibility.yaml" {
			continue
		}
		layer, err := image.LayerByDigest(descriptor.Digest)
		if err != nil {
			return nil, fmt.Errorf("failed to get compatibility.yaml layer: %w", err)
		}
		content, err := layer.Compressed()
		if err != nil {
			return nil, fmt.Errorf("failed to extract compatibility.yaml layer: %w", err)
		}
		defer content.Close()
		data, err := io.ReadAll(content)
		if err != nil {
			return nil, fmt.Errorf("failed to read compatibility.yaml: %w", err)
		}
		return data, nil
	}

	return nil, fmt.Errorf("no compatibility.yaml layer found in artifact")
}
