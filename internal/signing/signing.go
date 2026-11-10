// Package signing implements HMAC-SHA256 webhook signatures.
//
// The signed string is "<unix-timestamp>.<raw body>", so a captured payload
// cannot be replayed with a fresh timestamp.
package signing

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
	"time"
)

const Prefix = "v1="

// Sign returns the value for the Webhook-Signature header.
func Sign(secret string, ts int64, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(strconv.FormatInt(ts, 10)))
	mac.Write([]byte("."))
	mac.Write(body)
	return Prefix + hex.EncodeToString(mac.Sum(nil))
}

// Verify checks a signature header and that the timestamp is within tolerance of now.
func Verify(secret, header string, ts int64, body []byte, now time.Time, tolerance time.Duration) bool {
	if !strings.HasPrefix(header, Prefix) {
		return false
	}
	age := now.Sub(time.Unix(ts, 0))
	if age < -tolerance || age > tolerance {
		return false
	}
	return hmac.Equal([]byte(header), []byte(Sign(secret, ts, body)))
}
