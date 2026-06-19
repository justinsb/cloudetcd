// Copyright 2026 Justin Santa Barbara
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package e2e

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// pki holds the certificates and keys needed to run kube-apiserver and to
// authenticate to it as a cluster admin. It is the in-Go equivalent of the
// openssl recipe in docs/start-kube.md.
type pki struct {
	// CA certificate (PEM), used both as the apiserver's client-ca and as the
	// CA the client trusts for the serving certificate.
	caCertPEM []byte

	// Admin client credentials (PEM), CN=admin, O=system:masters.
	adminCertPEM []byte
	adminKeyPEM  []byte

	// On-disk paths passed to kube-apiserver flags.
	caCertFile      string
	servingCertFile string
	servingKeyFile  string
	saKeyFile       string
}

// generatePKI creates a CA, an apiserver serving certificate, a service-account
// signing key, and an admin client certificate, writing the files kube-apiserver
// needs into dir.
func generatePKI(t *testing.T, dir string) *pki {
	t.Helper()

	notBefore := time.Now().Add(-time.Hour)
	notAfter := time.Now().Add(24 * time.Hour)

	// Certificate authority.
	caKey := genKey(t)
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "cloudetcd-e2e-ca"},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	caDER := createCert(t, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("parse CA cert: %v", err)
	}
	caCertPEM := encodeCertPEM(caDER)

	// API server serving certificate.
	servingKey := genKey(t)
	servingTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "kube-apiserver"},
		NotBefore:    notBefore,
		NotAfter:     notAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames: []string{
			"localhost",
			"kubernetes",
			"kubernetes.default",
			"kubernetes.default.svc",
			"kubernetes.default.svc.cluster.local",
		},
		IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
	}
	servingDER := createCert(t, servingTmpl, caCert, &servingKey.PublicKey, caKey)

	// Admin client certificate: the system:masters group is hard-wired to
	// cluster-admin, so this authenticates as a full admin.
	adminKey := genKey(t)
	adminTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject:      pkix.Name{CommonName: "admin", Organization: []string{"system:masters"}},
		NotBefore:    notBefore,
		NotAfter:     notAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	adminDER := createCert(t, adminTmpl, caCert, &adminKey.PublicKey, caKey)

	// Service-account signing key. The same private key serves as both the
	// signing key and the (public) verification key file.
	saKey := genKey(t)

	write := func(name string, b []byte) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, b, 0o600); err != nil {
			t.Fatalf("write %s: %v", p, err)
		}
		return p
	}

	return &pki{
		caCertPEM:       caCertPEM,
		adminCertPEM:    encodeCertPEM(adminDER),
		adminKeyPEM:     encodeKeyPEM(adminKey),
		caCertFile:      write("ca.crt", caCertPEM),
		servingCertFile: write("serving.crt", encodeCertPEM(servingDER)),
		servingKeyFile:  write("serving.key", encodeKeyPEM(servingKey)),
		saKeyFile:       write("sa.key", encodeKeyPEM(saKey)),
	}
}

func genKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	k, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate RSA key: %v", err)
	}
	return k
}

func createCert(t *testing.T, tmpl, parent *x509.Certificate, pub *rsa.PublicKey, signer *rsa.PrivateKey) []byte {
	t.Helper()
	der, err := x509.CreateCertificate(rand.Reader, tmpl, parent, pub, signer)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	return der
}

func encodeCertPEM(der []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func encodeKeyPEM(key *rsa.PrivateKey) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
}
