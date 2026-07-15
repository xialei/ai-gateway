package server

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestWriteErrorEscapesQuotes is the regression for the malformed-JSON / field
// injection bug: writeError used to concatenate msg raw into a JSON string
// literal, so a backend error body containing a double quote produced invalid
// JSON (and let a malicious backend inject arbitrary fields). It must now
// encode via encoding/json so quotes and backslashes are escaped.
func TestWriteErrorEscapesQuotes(t *testing.T) {
	msg := `backend status 502: {"error":"overloaded"}`
	rec := httptest.NewRecorder()
	writeError(rec, 502, msg)

	var got struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("response is not valid JSON: %v (body=%q)", err, rec.Body.String())
	}
	if got.Error != msg {
		t.Errorf("error message not round-tripped: got %q want %q", got.Error, msg)
	}
	if strings.Contains(rec.Body.String(), `":"overloaded"}"`) {
		t.Errorf("raw quote leaked into body: %q", rec.Body.String())
	}
}

// TestWriteErrorEscapesBackslash covers the other JSON metachar.
func TestWriteErrorEscapesBackslash(t *testing.T) {
	msg := `path \ to \ thing`
	rec := httptest.NewRecorder()
	writeError(rec, 400, msg)
	var got struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("invalid JSON for backslash input: %v (body=%q)", err, rec.Body.String())
	}
	if got.Error != msg {
		t.Errorf("got %q want %q", got.Error, msg)
	}
}
