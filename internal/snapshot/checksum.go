package snapshot

import (
	"crypto/sha256"
	"encoding/hex"
)

// SHA256Hex returns the lowercase SHA-256 checksum for data.
func SHA256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
