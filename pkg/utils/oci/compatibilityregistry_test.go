// SPDX-FileCopyrightText: Contributors to the Gardener project
//
// SPDX-License-Identifier: Apache-2.0

package oci

import (
	"context"
	"encoding/pem"
	"io"
	"log"
	"net/http/httptest"
	"strings"

	"github.com/google/go-containerregistry/pkg/name"
	registryserver "github.com/google/go-containerregistry/pkg/registry"
	gcrv1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/static"
	"github.com/google/go-containerregistry/pkg/v1/types"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"

	gardencorev1 "github.com/gardener/gardener/pkg/apis/core/v1"
	v1beta1constants "github.com/gardener/gardener/pkg/apis/core/v1beta1/constants"
)

var _ = Describe("CompatibilityRegistry", func() {
	var (
		data  = []byte("builtWith:\n  gardener/gardener: v1.153.0\n")
		image gcrv1.Image
	)

	BeforeEach(func() {
		var err error
		image, err = mutate.Append(empty.Image, mutate.Addendum{
			Layer:       static.NewLayer(data, types.MediaType(mediaTypeCompatibility)),
			Annotations: map[string]string{"org.opencontainers.image.title": "compatibility.yaml"},
		})
		Expect(err).NotTo(HaveOccurred())
		image = mutate.MediaType(image, types.OCIManifestSchema1)
	})

	It("extracts the named compatibility.yaml layer alongside other files", func() {
		var err error
		image, err = mutate.Append(image, mutate.Addendum{
			Layer:       static.NewLayer([]byte("other data"), types.MediaType(mediaTypeCompatibility)),
			Annotations: map[string]string{"org.opencontainers.image.title": "other.yaml"},
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(extractCompatibilityLayer(image)).To(Equal(data))
	})

	It("fails when the artifact does not contain compatibility.yaml", func() {
		Expect(extractCompatibilityLayer(empty.Image)).Error().To(MatchError("no compatibility.yaml layer found in artifact"))
	})

	It("does not interpret an unnamed YAML layer as compatibility.yaml", func() {
		image, err := mutate.AppendLayers(empty.Image, static.NewLayer(data, types.MediaType(mediaTypeCompatibility)))
		Expect(err).NotTo(HaveOccurred())
		Expect(extractCompatibilityLayer(image)).Error().To(MatchError("no compatibility.yaml layer found in artifact"))
	})

	DescribeTable("discovers compatibility artifacts and refreshes cached results", func(referrersAPI bool) {
		server := httptest.NewTLSServer(registryserver.New(registryserver.WithReferrersSupport(referrersAPI), registryserver.Logger(log.New(io.Discard, "", 0))))
		DeferCleanup(server.Close)
		opts := []remote.Option{remote.WithTransport(server.Client().Transport)}
		ref, err := name.ParseReference(strings.TrimPrefix(server.URL, "https://") + "/extensions/example:v0.0.2")
		Expect(err).NotTo(HaveOccurred())
		root, err := mutate.Append(empty.Image, mutate.Addendum{
			Layer:       static.NewLayer([]byte("apiVersion: operator.gardener.cloud/v1alpha1\nkind: Extension\n"), types.MediaType("application/gardener.extension+yaml")),
			Annotations: map[string]string{"org.opencontainers.image.title": "extension.yaml"},
		})
		Expect(err).NotTo(HaveOccurred())
		root = mutate.MediaType(root, types.OCIManifestSchema1)
		Expect(remote.Write(ref, root, opts...)).To(Succeed())
		digest, err := root.Digest()
		Expect(err).NotTo(HaveOccurred())
		size, err := root.Size()
		Expect(err).NotTo(HaveOccurred())
		subject := gcrv1.Descriptor{Digest: digest, MediaType: types.OCIManifestSchema1, Size: size}

		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "compatibility-ca", Namespace: v1beta1constants.GardenNamespace},
			Data:       map[string][]byte{"bundle.crt": pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})},
		}
		registry := NewCompatibilityRegistry(fakeclient.NewClientBuilder().WithObjects(secret).Build())
		cache := &recordingCache{cache: newCache()}
		registry.registry.cache = cache
		repository := &gardencorev1.OCIRepository{Ref: new(ref.Name()), CABundleSecretRef: &corev1.LocalObjectReference{Name: secret.Name}}
		Expect(registry.Pull(context.Background(), repository)).Error().To(MatchError(ErrNoCompatibilityArtifact))

		attach := func(tag string, artifact gcrv1.Image, artifactType, created string) {
			GinkgoHelper()
			artifact = mutate.ConfigMediaType(artifact, types.MediaType(artifactType))
			artifact = mutate.Annotations(artifact, map[string]string{"org.opencontainers.image.created": created}).(gcrv1.Image)
			artifact = mutate.Subject(artifact, subject).(gcrv1.Image)
			Expect(remote.Write(ref.Context().Tag(tag), artifact, opts...)).To(Succeed())
		}
		attach("unrelated", image, "application/example.signature", "2026-11-11T12:00:00Z")
		Expect(registry.Pull(context.Background(), repository)).Error().To(MatchError(ErrNoCompatibilityArtifact))
		attach("compatibility-1", image, mediaTypeCompatibility, "2026-11-11T10:00:00Z")
		Expect(registry.Pull(context.Background(), repository)).To(Equal(data))
		Expect(cache.cacheHits).To(Equal(0))
		repository.Ref = new(ref.Context().Digest(digest.String()).Name())
		Expect(registry.Pull(context.Background(), repository)).To(Equal(data))
		Expect(cache.cacheHits).To(Equal(1))

		// Attach newer test results to the same extension digest. They must supersede cached compatibility data.
		newData := []byte("testedWith:\n  gardener/gardener: v1.154.1\n")
		newImage, err := mutate.Append(empty.Image, mutate.Addendum{
			Layer:       static.NewLayer(newData, types.MediaType(mediaTypeCompatibility)),
			Annotations: map[string]string{"org.opencontainers.image.title": "compatibility.yaml"},
		})
		Expect(err).NotTo(HaveOccurred())
		newImage = mutate.MediaType(newImage, types.OCIManifestSchema1)
		attach("compatibility-2", newImage, mediaTypeCompatibility, "2026-11-11T11:00:00Z")
		Expect(registry.Pull(context.Background(), repository)).To(Equal(newData))
		Expect(registry.Pull(context.Background(), repository)).To(Equal(newData))
		Expect(cache.cacheHits).To(Equal(2))

		// Moving the extension tag to another artifact must not reuse the previous attachment.
		newRoot, err := mutate.AppendLayers(root, static.NewLayer([]byte("new release"), types.MediaType("application/gardener.extension+yaml")))
		Expect(err).NotTo(HaveOccurred())
		Expect(remote.Write(ref, newRoot, opts...)).To(Succeed())
		repository.Ref = new(ref.Name())
		Expect(registry.Pull(context.Background(), repository)).Error().To(MatchError(ErrNoCompatibilityArtifact))
	},
		Entry("using the OCI referrers API", true),
		Entry("using the referrers tag fallback used by GHCR", false),
	)
})
