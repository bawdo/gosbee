package gmail

import (
	"context"
	"encoding/base64"
	"errors"
	"strings"
	"testing"

	"google.golang.org/api/googleapi"
)

// validServiceAccountJSON returns a minimal but well-formed service-account
// JSON sufficient for google.JWTConfigFromJSON to parse. The private key is
// throwaway; unit tests never invoke the TokenSource (which is what would
// actually parse and use the key).
func validServiceAccountJSON() []byte {
	return []byte(`{
		"type": "service_account",
		"project_id": "test",
		"private_key_id": "k1",
		"private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQC=\n-----END PRIVATE KEY-----\n",
		"client_email": "tester@test.iam.gserviceaccount.com",
		"client_id": "1234567890",
		"auth_uri": "https://accounts.google.com/o/oauth2/auth",
		"token_uri": "https://oauth2.googleapis.com/token"
	}`)
}

func TestNewSenderRejectsMalformedJSON(t *testing.T) {
	_, err := NewSender(context.Background(), []byte("not json"), "alice@example.com")
	if err == nil {
		t.Fatal("expected error for malformed JSON")
	}
}

func TestNewSenderRejectsEmptySubject(t *testing.T) {
	_, err := NewSender(context.Background(), validServiceAccountJSON(), "")
	if err == nil {
		t.Fatal("expected error for empty subjectUser")
	}
}

func TestNewSenderAcceptsValidJSON(t *testing.T) {
	s, err := NewSender(context.Background(), validServiceAccountJSON(), "alice@example.com")
	if err != nil {
		t.Fatalf("NewSender: %v", err)
	}
	if s == nil {
		t.Fatal("Sender was nil")
	}
}

func TestValidateServiceAccountJSONOK(t *testing.T) {
	if err := ValidateServiceAccountJSON(validServiceAccountJSON()); err != nil {
		t.Errorf("expected nil, got %v", err)
	}
}

func TestValidateServiceAccountJSONRejectsMalformed(t *testing.T) {
	if err := ValidateServiceAccountJSON([]byte("not json")); err == nil {
		t.Fatal("expected error")
	}
}

func TestValidateServiceAccountJSONRejectsWrongType(t *testing.T) {
	j := []byte(`{"type":"authorized_user","client_email":"x@y"}`)
	err := ValidateServiceAccountJSON(j)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "service_account") {
		t.Errorf("error should mention service_account; got %v", err)
	}
}

func TestValidateServiceAccountJSONRejectsMissingClientEmail(t *testing.T) {
	j := []byte(`{"type":"service_account"}`)
	err := ValidateServiceAccountJSON(j)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "client_email") {
		t.Errorf("error should mention client_email; got %v", err)
	}
}

func TestClassifyAPIErrorMaps401(t *testing.T) {
	err := classifyAPIError(&googleapi.Error{Code: 401, Message: "bad creds"})
	if !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("expected ErrUnauthorized; got %v", err)
	}
}

func TestClassifyAPIErrorMaps403(t *testing.T) {
	err := classifyAPIError(&googleapi.Error{Code: 403})
	if !errors.Is(err, ErrForbidden) {
		t.Fatalf("expected ErrForbidden; got %v", err)
	}
}

func TestClassifyAPIErrorMaps429(t *testing.T) {
	err := classifyAPIError(&googleapi.Error{Code: 429})
	if !errors.Is(err, ErrRateLimited) {
		t.Fatalf("expected ErrRateLimited; got %v", err)
	}
}

func TestClassifyAPIErrorMaps5xx(t *testing.T) {
	err := classifyAPIError(&googleapi.Error{Code: 503})
	if !errors.Is(err, ErrUpstream) {
		t.Fatalf("expected ErrUpstream; got %v", err)
	}
}

func TestClassifyAPIErrorPreservesGoogleapiInChain(t *testing.T) {
	orig := &googleapi.Error{Code: 401, Message: "go away"}
	err := classifyAPIError(orig)
	var ge *googleapi.Error
	if !errors.As(err, &ge) {
		t.Fatalf("googleapi.Error lost from chain: %v", err)
	}
	if ge.Code != 401 || !strings.Contains(ge.Message, "go away") {
		t.Errorf("wrong googleapi.Error: %+v", ge)
	}
}

func TestClassifyAPIErrorPassesNonGoogleapiThrough(t *testing.T) {
	orig := errors.New("network down")
	got := classifyAPIError(orig)
	if !errors.Is(got, orig) {
		t.Errorf("expected pass-through; got %v", got)
	}
}

func TestRawEncodingIsURLSafeNoPadding(t *testing.T) {
	// All 256 byte values round-trip via the encoding NewSender uses for Raw.
	var src [256]byte
	for i := range src {
		src[i] = byte(i)
	}
	enc := base64.URLEncoding.WithPadding(base64.NoPadding).EncodeToString(src[:])
	if strings.ContainsAny(enc, "+/=") {
		t.Errorf("encoding contains non-url-safe chars: %q", enc)
	}
	dec, err := base64.URLEncoding.WithPadding(base64.NoPadding).DecodeString(enc)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if string(dec) != string(src[:]) {
		t.Fatal("round-trip mismatch")
	}
}
