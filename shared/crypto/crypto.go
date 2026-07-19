// Package crypto provides cryptographic utilities for AIEN services.
//
// WHY ASYMMETRIC CRYPTOGRAPHY?
// =============================
// Asymmetric (public-key) cryptography uses a pair of keys:
// 1. Private Key: Kept secret by the user. Used to "sign" messages.
// 2. Public Key: Shared with anyone. Used to "verify" signatures.
//
// This guarantees:
// - Authentication: Only the holder of the private key could create the signature.
// - Integrity: If the message payload is altered in transit, verification fails.
// - Non-repudiation: The sender cannot deny sending the message.
//
// WHY HEX ENCODING?
// =================
// Cryptographic keys and signatures are raw binary bytes (usually 32 or 64 bytes).
// Transporting raw binary bytes through JSON APIs or Protobuf string fields is
// problematic because certain byte sequences represent invalid UTF-8 characters.
// We encode binary bytes into hexadecimal strings (0-9, a-f) which are safe for
// all networks and APIs.
//
// Alternatively, Base64 encoding could be used (it is ~33% more space-efficient),
// but Hexadecimal is simpler to debug, read, and cross-verify with command-line
// tools (like openssl or certutil).
package crypto

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

// GenerateKeyPair generates a new random Ed25519 keypair.
//
// Returns:
//   - string: Public key hex encoded (64 characters).
//   - string: Private key hex encoded (128 characters).
//   - error: If random source fails.
func GenerateKeyPair() (string, string, error) {
	// ed25519.GenerateKey utilizes crypto/rand (cryptographically secure pseudo-random number generator).
	pubKey, privKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return "", "", fmt.Errorf("failed to generate key pair: %w", err)
	}

	pubHex := hex.EncodeToString(pubKey)
	privHex := hex.EncodeToString(privKey)

	return pubHex, privHex, nil
}

// Sign signs a message payload using the hex-encoded private key.
//
// Parameters:
//   - payload: the binary bytes to sign.
//   - privateKeyHex: the hex-encoded Ed25519 private key.
//
// Returns:
//   - string: the hex-encoded signature.
//   - error: if the key is invalid.
func Sign(payload []byte, privateKeyHex string) (string, error) {
	privKeyBytes, err := hex.DecodeString(privateKeyHex)
	if err != nil {
		return "", fmt.Errorf("failed to decode private key hex: %w", err)
	}

	if len(privKeyBytes) != ed25519.PrivateKeySize {
		return "", fmt.Errorf("invalid private key size: expected %d bytes, got %d", ed25519.PrivateKeySize, len(privKeyBytes))
	}

	// Cast the slice to ed25519.PrivateKey
	privKey := ed25519.PrivateKey(privKeyBytes)
	sig := ed25519.Sign(privKey, payload)

	return hex.EncodeToString(sig), nil
}

// Verify checks if a signature is valid for the payload and public key.
//
// Parameters:
//   - payload: the signed binary bytes.
//   - signatureHex: the hex-encoded signature.
//   - publicKeyHex: the hex-encoded Ed25519 public key.
//
// Returns:
//   - bool: true if the signature is valid, false otherwise.
//   - error: if signature or public key hex is malformed.
func Verify(payload []byte, signatureHex, publicKeyHex string) (bool, error) {
	pubKeyBytes, err := hex.DecodeString(publicKeyHex)
	if err != nil {
		return false, fmt.Errorf("failed to decode public key hex: %w", err)
	}

	if len(pubKeyBytes) != ed25519.PublicKeySize {
		return false, fmt.Errorf("invalid public key size: expected %d bytes, got %d", ed25519.PublicKeySize, len(pubKeyBytes))
	}

	sigBytes, err := hex.DecodeString(signatureHex)
	if err != nil {
		return false, fmt.Errorf("failed to decode signature hex: %w", err)
	}

	if len(sigBytes) != ed25519.SignatureSize {
		return false, fmt.Errorf("invalid signature size: expected %d bytes, got %d", ed25519.SignatureSize, len(sigBytes))
	}

	pubKey := ed25519.PublicKey(pubKeyBytes)
	isValid := ed25519.Verify(pubKey, payload, sigBytes)

	return isValid, nil
}

// GetSignBytes returns a deterministic byte representation of an intent
// for signing and verifying.
//
// Format:
//   "intent_type|submitter_id|payload_string"
//
// By using a simple pipe-delimited format, we ensure that both Go client
// binaries and web interfaces (e.g. JavaScript using @noble/curves) can
// reconstruct the exact same byte sequence.
func GetSignBytes(typeStr, submitterId string, payload []byte) []byte {
	return []byte(fmt.Sprintf("%s|%s|%s", typeStr, submitterId, string(payload)))
}
