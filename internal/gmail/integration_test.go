package gmail

import (
	"context"
	"os"
	"testing"
	"time"
)

// integrationRecipient is the ONLY address this integration suite will ever
// send to. DO NOT make this configurable - it is an invariant. A developer
// running this suite should never be one typo away from emailing a real
// customer or teammate.
const integrationRecipient = "k@flip.com.au"

func TestIntegrationSend(t *testing.T) {
	if os.Getenv("JELLYFISH_GMAIL_TESTS") != "1" {
		t.Skip("set JELLYFISH_GMAIL_TESTS=1 to run the live Gmail send integration test")
	}
	jsonPath := os.Getenv("JELLYFISH_GMAIL_TEST_JSON")
	if jsonPath == "" {
		t.Fatal("JELLYFISH_GMAIL_TEST_JSON must be a path to a service-account JSON with DWD configured")
	}
	subject := os.Getenv("JELLYFISH_GMAIL_TEST_FROM")
	if subject == "" {
		t.Fatal("JELLYFISH_GMAIL_TEST_FROM must be the Workspace user the service account can impersonate")
	}

	saJSON, err := os.ReadFile(jsonPath) // #nosec G304,G703 - test-only, operator-provided path
	if err != nil {
		t.Fatalf("read service-account JSON: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sender, err := NewSender(ctx, saJSON, subject)
	if err != nil {
		t.Fatalf("NewSender: %v", err)
	}

	body := "From: " + subject + "\r\n" +
		"To: " + integrationRecipient + "\r\n" +
		"Subject: jellyfish integration-test " + time.Now().UTC().Format(time.RFC3339) + "\r\n" +
		"\r\nIntegration-test message - safe to delete.\r\n"

	id, err := sender.Send(ctx, []byte(body))
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if id == "" {
		t.Fatal("empty Gmail message id returned")
	}
}
