package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestWritePairPage(t *testing.T) {
	dir := t.TempDir()
	qr := filepath.Join(dir, "qr.png")
	page := filepath.Join(dir, "index.html")
	now := time.Now()

	writePairPage(qr, pairingStatus{Active: true, Session: 3, Phone: "917000189190", Code: "ABCD-EFGH", QRAt: now, ExpiresAt: now.Add(pairSessionLife)})
	b, err := os.ReadFile(page)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"ABCD-EFGH", "+917000189190", "Link with phone number instead", "qr.png?v=", "Login session 3"} {
		if !strings.Contains(string(b), want) {
			t.Errorf("pairing page lacks %q", want)
		}
	}

	writePairPage(qr, pairingStatus{Active: true, Session: 4, Phone: "917000189190", CodeError: "rate-overlimit"})
	b, _ = os.ReadFile(page)
	if !strings.Contains(string(b), "rate-overlimit") || strings.Contains(string(b), "qr.png?v=") {
		t.Errorf("code-error page without a QR yet is wrong:\n%s", b)
	}

	writePairPage(qr, pairingStatus{LinkedPhone: "917000189190", ConnectedAt: now})
	b, _ = os.ReadFile(page)
	if !strings.Contains(string(b), "WhatsApp bridge linked") || strings.Contains(string(b), "ABCD-EFGH") {
		t.Errorf("linked page is wrong:\n%s", b)
	}
}

func TestWritePairPageNeedsExistingDir(t *testing.T) {
	qr := filepath.Join(t.TempDir(), "missing", "qr.png")
	writePairPage(qr, pairingStatus{Active: true})
	if _, err := os.Stat(filepath.Dir(qr)); !os.IsNotExist(err) {
		t.Fatalf("writePairPage must not create %s (err=%v)", filepath.Dir(qr), err)
	}
}

func TestKnownPairPhoneAndMarkLinked(t *testing.T) {
	t.Chdir(t.TempDir()) // pairPhoneFile is relative to the working directory
	t.Setenv("WHATSAPP_QR_PATH", filepath.Join(t.TempDir(), "qr.png"))
	t.Setenv("WHATSAPP_PAIR_PHONE", "")
	if err := os.Mkdir("store", 0o755); err != nil {
		t.Fatal(err)
	}

	if got := knownPairPhone(); got != "" {
		t.Fatalf("knownPairPhone() = %q before any link, want empty", got)
	}
	markLinked("917000189190")
	if got := knownPairPhone(); got != "917000189190" {
		t.Fatalf("knownPairPhone() = %q after markLinked, want 917000189190", got)
	}
	if s := currentPairingStatus(); s.Active || s.LinkedPhone != "917000189190" || s.ConnectedAt.IsZero() {
		t.Fatalf("status after markLinked = %+v", s)
	}

	// A different account must never be recorded (the safety gate unlinks it).
	markLinked("15551234567")
	if got := knownPairPhone(); got != "917000189190" {
		t.Fatalf("knownPairPhone() = %q after linking another account, want 917000189190", got)
	}
	if s := currentPairingStatus(); s.LinkedPhone != "917000189190" {
		t.Fatalf("status shows the other account as linked: %+v", s)
	}

	t.Setenv("WHATSAPP_PAIR_PHONE", "+91 98765-43210")
	if got := knownPairPhone(); got != "919876543210" {
		t.Fatalf("knownPairPhone() = %q with WHATSAPP_PAIR_PHONE set, want 919876543210", got)
	}
}
