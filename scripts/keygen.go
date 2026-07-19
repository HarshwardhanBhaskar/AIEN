// keygen.go
//
// AIEN Cryptographic Helper Utility
// ==================================
// This is a utility script to help developers generate Ed25519 keypairs,
// format sign payloads, sign intents, and generate ready-to-use JSON requests.
//
// Usage 1: Generate Keypair
//   go run scripts/keygen.go
//
// Usage 2: Sign a Payload
//   go run scripts/keygen.go --sign <privkey_hex> <type> <submitter_id> <payload_json>
package main

import (
	"encoding/hex"
	"fmt"
	"os"

	"github.com/aien-platform/aien/shared/crypto"
)

func main() {
	args := os.Args[1:]

	// Case 1: Sign an intent request
	if len(args) > 0 && args[0] == "--sign" {
		if len(args) < 5 {
			fmt.Println("Error: Missing arguments for signing.")
			fmt.Println("Usage: go run scripts/keygen.go --sign <privkey_hex> <type> <submitter_id> <payload_json>")
			os.Exit(1)
		}

		privKeyHex := args[1]
		intentType := args[2]
		submitterID := args[3]
		payload := args[4]

		// 1. Reconstruct public key from private key to print it for the user
		privBytes, err := hex.DecodeString(privKeyHex)
		if err != nil || len(privBytes) != 64 {
			fmt.Printf("Error: Invalid private key hex format (expected 128 characters/64 bytes): %v\n", err)
			os.Exit(1)
		}
		pubKeyHex := hex.EncodeToString(privBytes[32:])

		// 2. Generate signature
		signBytes := crypto.GetSignBytes(intentType, submitterID, []byte(payload))
		signature, err := crypto.Sign(signBytes, privKeyHex)
		if err != nil {
			fmt.Printf("Error signing payload: %v\n", err)
			os.Exit(1)
		}

		// 3. Output completed JSON payload
		fmt.Println("\n================================================================================")
		fmt.Println("🔒 DETERMINISTIC SIGN BYTES:")
		fmt.Println(string(signBytes))
		fmt.Println("================================================================================")
		fmt.Println("\n🚀 COMPLETED REST JSON PAYLOAD:")
		fmt.Println("{")
		fmt.Printf("  \"type\": \"%s\",\n", intentType)
		fmt.Printf("  \"submitter_id\": \"%s\",\n", submitterID)
		fmt.Printf("  \"payload\": %q,\n", payload)
		fmt.Printf("  \"signature\": \"%s\",\n", signature)
		fmt.Printf("  \"submitter_public_key\": \"%s\"\n", pubKeyHex)
		fmt.Println("}")
		fmt.Println("================================================================================")
		os.Exit(0)
	}

	// Case 2: Generate keys
	pubHex, privHex, err := crypto.GenerateKeyPair()
	if err != nil {
		fmt.Printf("Failed to generate keys: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("\n================================================================================")
	fmt.Println("🔑 NEW ED25519 KEYPAIR GENERATED")
	fmt.Println("================================================================================")
	fmt.Printf("Public Key (Hex):  %s\n", pubHex)
	fmt.Printf("Private Key (Hex): %s\n", privHex)
	fmt.Println("================================================================================")
	fmt.Println("\nTo sign a request, run:")
	fmt.Printf("go run scripts/keygen.go --sign %s TRANSFER user-alice-001 '{\\\"from\\\":\\\"alice\\\",\\\"to\\\":\\\"bob\\\",\\\"amount\\\":100}'\n", privHex)
	fmt.Println("================================================================================")
}
