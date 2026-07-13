package server

import (
	"crypto/rand"
	"encoding/hex"
	"net"
)

// newRequestID returns a short random correlation id. (time-based ordering
// is not required; collisions are acceptable for trace correlation.)
func newRequestID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return "req_" + hex.EncodeToString(b[:])
}

// newDisposableListener returns a free TCP listener on an ephemeral port,
// used to host the in-process mock backend.
func newDisposableListener() net.Listener {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		// fall back to any free port; extremely unlikely to fail here
		ln, err = net.Listen("tcp", ":0")
		if err != nil {
			panic("cannot bind ephemeral port for mock backend: " + err.Error())
		}
	}
	return ln
}
