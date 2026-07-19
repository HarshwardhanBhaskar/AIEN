package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

func main() {
	outDir := "./certs"
	if err := os.MkdirAll(outDir, 0755); err != nil {
		fmt.Printf("Failed to create certs directory: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Generating CA Certificate...")
	caKey, caCertBytes, err := generateCA(outDir)
	if err != nil {
		fmt.Printf("Failed to generate CA: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Generating Server Certificate (for intent-service)...")
	err = generateServerCert(outDir, caKey, caCertBytes)
	if err != nil {
		fmt.Printf("Failed to generate Server Cert: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Generating Client Certificate (for gateway)...")
	err = generateClientCert(outDir, caKey, caCertBytes)
	if err != nil {
		fmt.Printf("Failed to generate Client Cert: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("All certificates successfully generated in ./certs directory.")
}

func generateCA(outDir string) (*rsa.PrivateKey, []byte, error) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, nil, err
	}

	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		return nil, nil, err
	}

	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"AIEN Authority"},
			CommonName:   "AIEN Root CA",
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().AddDate(10, 0, 0), // 10 years
		IsCA:                  true,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		return nil, nil, err
	}

	if err := savePEM(filepath.Join(outDir, "ca.pem"), "CERTIFICATE", derBytes); err != nil {
		return nil, nil, err
	}

	return priv, derBytes, nil
}

func generateServerCert(outDir string, caKey *rsa.PrivateKey, caCertBytes []byte) error {
	caCert, err := x509.ParseCertificate(caCertBytes)
	if err != nil {
		return err
	}

	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return err
	}

	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		return err
	}

	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"AIEN Service"},
			CommonName:   "intent-service",
		},
		NotBefore:   time.Now(),
		NotAfter:    time.Now().AddDate(2, 0, 0), // 2 years
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:    []string{"localhost", "intent-service", "wallet", "wallet-service"},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("0.0.0.0")},
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, caCert, &priv.PublicKey, caKey)
	if err != nil {
		return err
	}

	if err := savePEM(filepath.Join(outDir, "server-cert.pem"), "CERTIFICATE", derBytes); err != nil {
		return err
	}

	privBytes, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return err
	}

	return savePEM(filepath.Join(outDir, "server-key.pem"), "PRIVATE KEY", privBytes)
}

func generateClientCert(outDir string, caKey *rsa.PrivateKey, caCertBytes []byte) error {
	caCert, err := x509.ParseCertificate(caCertBytes)
	if err != nil {
		return err
	}

	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return err
	}

	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		return err
	}

	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"AIEN Client"},
			CommonName:   "gateway",
		},
		NotBefore:   time.Now(),
		NotAfter:    time.Now().AddDate(2, 0, 0), // 2 years
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		DNSNames:    []string{"localhost", "gateway"},
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, caCert, &priv.PublicKey, caKey)
	if err != nil {
		return err
	}

	if err := savePEM(filepath.Join(outDir, "client-cert.pem"), "CERTIFICATE", derBytes); err != nil {
		return err
	}

	privBytes, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return err
	}

	return savePEM(filepath.Join(outDir, "client-key.pem"), "PRIVATE KEY", privBytes)
}

func savePEM(filename string, pemType string, derBytes []byte) error {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	block := &pem.Block{
		Type:  pemType,
		Bytes: derBytes,
	}

	return pem.Encode(f, block)
}
