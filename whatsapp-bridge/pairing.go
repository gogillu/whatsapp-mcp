package main

// pairing.go - links the bridge to its WhatsApp account (first run, or after
// WhatsApp logged the device out) without anyone watching its console.
//
// Before: the bridge paired only at process start (one ~160 s QR session,
// then "Timeout waiting for QR code scan" and exit), and when WhatsApp logged
// the device out at runtime the process stayed up serving a dead session. On
// 2026-10-28 16:39 IST WhatsApp removed the Personal bridge
// (<stream:error code="401"><conflict type="device_removed"/>); /api/status
// answered connected:false for hours and nothing restarted it.
//
// Now:
//   - events.LoggedOut makes the bridge exit (code 5, main.go) so its
//     supervisor restarts it straight into pairing mode.
//   - Pairing mode runs login sessions back to back until the device is
//     linked, so a fresh QR code is always on offer (WhatsApp closes each
//     login socket ~160 s after it opens).
//   - When the account's number is known (WHATSAPP_PAIR_PHONE, else
//     store/pair-phone.txt, which the bridge refreshes on every login) each
//     session also requests an 8-character phone-number linking code, so the
//     owner can re-link from the phone itself: WhatsApp > Linked devices >
//     Link a device > "Link with phone number instead".
//   - The live code + QR are published as an auto-refreshing index.html next
//     to qr.png, and as "pairing" in GET /api/status.
//   - Safety gate: a link to any number other than the known one is logged
//     out again at once (exit 6), so a stray scan of the QR can't take over
//     the bridge.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/mdp/qrterminal"
	qrgen "rsc.io/qr"

	"go.mau.fi/whatsmeow"
	waLog "go.mau.fi/whatsmeow/util/log"
)

const (
	pairPhoneFile    = "store/pair-phone.txt"
	pairSessionLife  = 160 * time.Second // QR refs per login socket: 60 s + 5 x 20 s
	pairCodeMaxSkip  = 16                // max sessions without a code request after failures (~45 min)
	exitLoggedOut    = 5
	exitWrongAccount = 6
)

// pairingStatus is what /api/status ("pairing") and the pairing page show.
type pairingStatus struct {
	Active      bool      `json:"active"`
	Session     int       `json:"session,omitempty"`
	Phone       string    `json:"phone,omitempty"`
	Code        string    `json:"code,omitempty"`
	CodeError   string    `json:"code_error,omitempty"`
	QRAt        time.Time `json:"qr_at,omitzero"`
	ExpiresAt   time.Time `json:"expires_at,omitzero"`
	LinkedPhone string    `json:"linked_phone,omitempty"`
	ConnectedAt time.Time `json:"connected_at,omitzero"`
}

var (
	pairMu     sync.Mutex
	pairStatus pairingStatus
	nonDigits  = regexp.MustCompile(`[^0-9]`)
)

func currentPairingStatus() pairingStatus {
	pairMu.Lock()
	defer pairMu.Unlock()
	return pairStatus
}

// setPairingStatus updates the shared status and republishes the page.
func setPairingStatus(update func(*pairingStatus)) {
	pairMu.Lock()
	defer pairMu.Unlock()
	update(&pairStatus)
	writePairPage(qrPNGPath(), pairStatus)
}

func qrPNGPath() string {
	if p := strings.TrimSpace(os.Getenv("WHATSAPP_QR_PATH")); p != "" {
		return p
	}
	return `C:\Users\arushi\apache-www\god\whatsapp\qr.png`
}

// pairPageURL is where the account owner finds the pairing page.
func pairPageURL() string {
	if u := strings.TrimSpace(os.Getenv("WHATSAPP_QR_URL")); u != "" {
		return u
	}
	return "https://gogillu.live/god/whatsapp/"
}

// knownPairPhone returns the number this bridge belongs to, if known:
// WHATSAPP_PAIR_PHONE, else the account it was last linked to.
func knownPairPhone() string {
	if p := nonDigits.ReplaceAllString(os.Getenv("WHATSAPP_PAIR_PHONE"), ""); p != "" {
		return p
	}
	b, _ := os.ReadFile(pairPhoneFile)
	return nonDigits.ReplaceAllString(string(b), "")
}

// markLinked records the linked account (so a later re-link can offer a
// pairing code and refuse other accounts) and flips the page to "linked".
// Called after pairing and on every (re)connect.
func markLinked(phone string) {
	phone = nonDigits.ReplaceAllString(phone, "")
	if phone == "" {
		return
	}
	if known := knownPairPhone(); known != "" && known != phone {
		// Another account: the pairing safety gate unlinks it. Recording it
		// here (Connected fires before the gate's logout) would let the
		// wrong account pass the gate after the restart.
		return
	}
	if b, err := os.ReadFile(pairPhoneFile); err != nil || nonDigits.ReplaceAllString(string(b), "") != phone {
		if err := os.WriteFile(pairPhoneFile, []byte(phone+"\n"), 0o600); err != nil {
			fmt.Printf("pairing: could not record the linked number in %s: %v\n", pairPhoneFile, err)
		}
	}
	setPairingStatus(func(s *pairingStatus) {
		*s = pairingStatus{LinkedPhone: phone, ConnectedAt: time.Now()}
	})
}

// pairUntilLinked offers QR codes - and pairing codes when the account's
// number is known - in back-to-back login sessions until the device is
// linked. It returns nil once linked; an error means the bridge should exit
// and let its supervisor start over.
func pairUntilLinked(client *whatsmeow.Client, logger waLog.Logger) error {
	qrPath := qrPNGPath()
	if err := os.MkdirAll(filepath.Dir(qrPath), 0o755); err != nil {
		logger.Warnf("pairing: cannot create %s: %v", filepath.Dir(qrPath), err)
	}
	phone := knownPairPhone()
	if phone == "" {
		logger.Warnf("pairing: account number unknown (set WHATSAPP_PAIR_PHONE or %s) - offering QR codes only, and any account may link", pairPhoneFile)
	}
	logger.Warnf("pairing: device is not linked - open %s to link it", pairPageURL())

	skip, backoff := 0, 0
	for session := 1; ; session++ {
		wantCode := phone != "" && skip == 0
		if skip > 0 {
			skip--
		}
		linked, codeErr, pause, err := runPairingSession(client, logger, session, phone, wantCode, qrPath)
		if err != nil {
			return err
		}
		if linked {
			return nil
		}
		if wantCode {
			if codeErr != nil {
				// WhatsApp rate-limits link-code requests per number; back off.
				backoff = min(max(2*backoff, 1), pairCodeMaxSkip)
				skip = backoff
				logger.Warnf("pairing: no pairing code for the next %d session(s) after: %v", skip, codeErr)
			} else {
				backoff = 0
			}
		}
		time.Sleep(pause)
	}
}

// runPairingSession runs one login socket (~160 s): publishes its QR codes,
// requests a pairing code if wanted, and reports whether the device got
// linked and how long to pause before the next session.
func runPairingSession(client *whatsmeow.Client, logger waLog.Logger, session int, phone string, wantCode bool, qrPath string) (linked bool, codeErr error, pause time.Duration, err error) {
	// Background context on purpose: cancelling the QR channel's context
	// makes whatsmeow disconnect the client, which must never hit a fresh link.
	qrChan, err := client.GetQRChannel(context.Background())
	if err != nil {
		return false, nil, 0, fmt.Errorf("pairing session %d: %w", session, err)
	}
	// A failed Connect leaves the QR channel's event handler registered (it
	// could later disconnect a good session), so give up on this process
	// instead of retrying in-process; the supervisor restarts us.
	if err := client.Connect(); err != nil {
		return false, nil, 0, fmt.Errorf("pairing session %d: connect: %w", session, err)
	}
	setPairingStatus(func(s *pairingStatus) {
		*s = pairingStatus{Active: true, Session: session, Phone: phone}
	})

	pause = 5 * time.Second
	first := true
	for evt := range qrChan {
		switch evt.Event {
		case whatsmeow.QRChannelEventCode:
			now := time.Now()
			if qr, err := qrgen.Encode(evt.Code, qrgen.M); err == nil {
				_ = os.WriteFile(qrPath, qr.PNG(), 0o644)
			}
			if !first {
				setPairingStatus(func(s *pairingStatus) { s.QRAt = now })
				continue
			}
			first = false
			if session == 1 {
				fmt.Println("\nScan this QR code with your WhatsApp app:")
				qrterminal.GenerateHalfBlock(evt.Code, qrterminal.L, os.Stdout)
			}
			// PairPhone needs the login socket up, i.e. after the first QR.
			code := ""
			if wantCode {
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				code, codeErr = client.PairPhone(ctx, phone, false, whatsmeow.PairClientChrome, "Chrome (Windows)")
				cancel()
			}
			setPairingStatus(func(s *pairingStatus) {
				s.QRAt, s.ExpiresAt, s.Code = now, now.Add(pairSessionLife), code
				if codeErr != nil {
					s.CodeError = codeErr.Error()
				}
			})
			switch {
			case code != "":
				logger.Infof("pairing: session %d - code %s for +%s (valid ~%d s): WhatsApp > Linked devices > Link a device > Link with phone number instead. QR: %s", session, code, phone, int(pairSessionLife.Seconds()), qrPath)
			case codeErr != nil:
				logger.Warnf("pairing: session %d - pairing code request failed: %v. QR: %s", session, codeErr, qrPath)
			default:
				logger.Infof("pairing: session %d - QR: %s", session, qrPath)
			}

		case whatsmeow.QRChannelSuccess.Event:
			got := ""
			if client.Store.ID != nil {
				got = client.Store.ID.User
			}
			if phone != "" && got != phone {
				refuseWrongAccount(client, logger, phone, got)
			}
			_ = os.Remove(qrPath)
			markLinked(got)
			fmt.Printf("\nSuccessfully linked to +%s!\n", got)
			return true, codeErr, 0, nil

		case whatsmeow.QRChannelTimeout.Event:
			logger.Infof("pairing: session %d ended without a link", session)

		case whatsmeow.QRChannelEventError:
			logger.Errorf("pairing: session %d failed: %v", session, evt.Error)
			pause = time.Minute

		case whatsmeow.QRChannelEventPasskeyRequest, whatsmeow.QRChannelEventPasskeyResponse:
			logger.Warnf("pairing: the phone offered passkey linking, which this bridge can't do - use the pairing code or the QR code")

		default: // err-unexpected-state, err-client-outdated, err-scanned-without-multidevice
			logger.Errorf("pairing: session %d ended with %s", session, evt.Event)
			pause = time.Minute
		}
	}
	client.Disconnect()
	return false, codeErr, pause, nil
}

// refuseWrongAccount unlinks a device that got linked to a number other than
// the one this bridge belongs to, then exits so the supervisor starts over.
func refuseWrongAccount(client *whatsmeow.Client, logger waLog.Logger, want, got string) {
	logger.Errorf("pairing: this bridge belongs to +%s but was linked to +%s - unlinking it again (delete %s to allow another account)", want, got, pairPhoneFile)
	waitLoggedIn(client, 30*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	err := client.Logout(ctx)
	cancel()
	if err != nil {
		logger.Errorf("pairing: logout failed (%v) - deleting the local session instead", err)
		_ = client.Store.Delete(context.Background())
	}
	os.Exit(exitWrongAccount)
}

// waitLoggedIn waits until the client has a logged-in connection.
func waitLoggedIn(client *whatsmeow.Client, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for !client.IsConnected() || !client.IsLoggedIn() {
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(250 * time.Millisecond)
	}
	return true
}

// requireLinked answers 503 for every REST call except /api/status while the
// bridge isn't linked: a fresh device has no contact/identity stores until it
// is paired, so most handlers would otherwise panic or fail obscurely.
func requireLinked(linked func() bool, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/status" && !linked() {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(SendMessageResponse{
				Message: "WhatsApp bridge is not linked to an account - waiting to be linked (see /api/status)",
			})
			return
		}
		next.ServeHTTP(w, r)
	})
}

// writePairPage publishes the status page as index.html next to qr.png,
// only if that directory exists (pairing mode creates it).
func writePairPage(qrPath string, s pairingStatus) {
	dir := filepath.Dir(qrPath)
	if st, err := os.Stat(dir); err != nil || !st.IsDir() {
		return
	}
	data := struct {
		pairingStatus
		ExpiresUnix int64
		SecondsLeft int
		QRVersion   int64
	}{pairingStatus: s}
	if !s.ExpiresAt.IsZero() {
		data.ExpiresUnix = s.ExpiresAt.Unix()
		data.SecondsLeft = max(0, int(time.Until(s.ExpiresAt).Seconds()))
	}
	if !s.QRAt.IsZero() {
		data.QRVersion = s.QRAt.Unix()
	}
	var buf bytes.Buffer
	if err := pairPageTmpl.Execute(&buf, data); err != nil {
		fmt.Printf("pairing: cannot render the pairing page: %v\n", err)
		return
	}
	page := filepath.Join(dir, "index.html")
	tmp := page + ".tmp"
	if err := os.WriteFile(tmp, buf.Bytes(), 0o644); err == nil {
		if os.Rename(tmp, page) == nil {
			return
		}
		_ = os.Remove(tmp)
	}
	// Rename can fail on Windows while the web server is reading the page.
	_ = os.WriteFile(page, buf.Bytes(), 0o644)
}

var pairPageTmpl = template.Must(template.New("pair").Parse(`<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="refresh" content="10">
<title>WhatsApp bridge{{if .Active}} - link device{{end}}</title>
<style>
body{font-family:system-ui,sans-serif;max-width:34rem;margin:1.5rem auto;padding:0 1rem;color:#111;line-height:1.45}
.code{font:700 2.4rem/1.2 ui-monospace,Menlo,Consolas,monospace;letter-spacing:.12em;text-align:center;background:#e9f9ef;border:2px solid #25d366;border-radius:.6rem;padding:.6rem;margin:.8rem 0;user-select:all}
.muted{color:#666;font-size:.9rem}
img{width:16rem;height:16rem;image-rendering:pixelated}
</style></head><body>
{{if .Active}}
<h1>Link the WhatsApp bridge</h1>
{{if .Code}}
<p>On the phone of <b>+{{.Phone}}</b>: WhatsApp &rarr; &#8942; &rarr; <b>Linked devices</b> &rarr; <b>Link a device</b> &rarr; <b>Link with phone number instead</b>, then enter:</p>
<div class="code">{{.Code}}</div>
<p class="muted">Expires in <b id="left">{{.SecondsLeft}}</b> s. A new code appears here automatically (this page reloads every 10 s).</p>
{{else if .CodeError}}
<p>No pairing code right now ({{.CodeError}}) - use the QR code below, or wait for the next one.</p>
{{else if .Phone}}
<p>Getting a pairing code for +{{.Phone}}&hellip;</p>
{{end}}
<h2>Or scan from another screen</h2>
{{if .QRVersion}}<p><img src="qr.png?v={{.QRVersion}}" alt="WhatsApp link QR code"></p>{{end}}
<p class="muted">WhatsApp &rarr; Linked devices &rarr; Link a device &rarr; scan. Login session {{.Session}}.</p>
{{else if .LinkedPhone}}
<h1>&#9989; WhatsApp bridge linked</h1>
<p>Linked to <b>+{{.LinkedPhone}}</b> &middot; connected {{.ConnectedAt.Format "02 Jan 2006 15:04:05 MST"}}.</p>
{{else}}
<h1>WhatsApp bridge starting&hellip;</h1>
{{end}}
<script>
(function(){var t={{.ExpiresUnix}},el=document.getElementById("left");if(!el||!t)return;
function tick(){el.textContent=Math.max(0,Math.round(t-Date.now()/1000));}tick();setInterval(tick,1000);})();
</script>
</body></html>
`))
