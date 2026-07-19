// Package config handles environment variable parsing for AIEN services.
//
// WHY ENVIRONMENT VARIABLES?
// ==========================
// In modern cloud-native architectures (like Kubernetes, Docker, Heroku),
// the "Twelve-Factor App" methodology dictates that configuration must be
// stored in the environment, NOT in files embedded in code.
//
// 1. Separation: Code remains identical across environments (local, dev, prod).
// 2. Security: Database passwords, TLS keys, and API tokens are never
//    checked into version control (git).
// 3. Simplicity: Container schedulers can dynamically inject configurations
//    without rebuilding images.
//
// DESIGN DECISION:
// We read environment variables using os.Getenv. If a variable is not set,
// we fall back to a sensible default. This makes local running (without Docker)
// work out of the box, while letting Docker Compose override them easily.
package config

import (
	"os"
)

// GetEnv retrieves the value of an environment variable or returns a fallback default.
//
// Parameters:
//   - key: the environment variable name (e.g., "DATABASE_URL").
//   - fallback: default value if the variable is empty or unset.
//
// Returns:
//   - string: the environment value or fallback.
func GetEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists && value != "" {
		return value
	}
	return fallback
}
