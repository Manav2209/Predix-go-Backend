package websocket

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	ws "github.com/gorilla/websocket"
	"predix/pkg/auth"
)

func TestServerRequiresTokenWhenConfigured(t *testing.T) {
	auth.Init("test-secret")

	token, err := auth.GenerateToken("user-1", "a@b.com")
	if err != nil {
		t.Fatal(err)
	}

	hub := NewHub()

	server := NewServer(hub)
	server.RequireAuth = true
	server.AllowQueryToken = true // Legacy compatibility requires explicit opt-in.

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", server.Handle)

	ts := httptest.NewServer(mux)
	defer ts.Close()

	cases := []struct {
		name  string
		query string
		want  int
	}{
		{"missing token", "", http.StatusUnauthorized},
		{"bad token", "?token=garbage", http.StatusUnauthorized},
		{"valid token", "?token=" + token, http.StatusBadRequest},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := http.Get(ts.URL + "/ws" + tc.query)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()

			body, _ := io.ReadAll(resp.Body)

			if resp.StatusCode != tc.want {
				t.Errorf("status = %d, want %d (body: %s)",
					resp.StatusCode, tc.want, body)
			}

			if tc.name == "missing token" &&
				!strings.Contains(string(body), "token") {
				t.Errorf("body = %q, want token hint", body)
			}
		})
	}
}

func TestWebSocketOriginRestriction(t *testing.T) {
	for _, tc := range []struct {
		name    string
		allowed []string
		origin  string
		want    bool
	}{
		{"no origin", nil, "", true},
		{"same origin", nil, "http://example.com", true},
		{"cross origin", nil, "http://evil.com", false},
		{"wrong scheme", nil, "https://example.com", false},
		{"null origin", nil, "null", false},
		{"allowlisted", []string{"https://frontend.com"}, "https://frontend.com", true},
		{"wrong port", []string{"https://frontend.com"}, "https://frontend.com:444", false},
		{"prefix attack", []string{"https://frontend.com"}, "https://frontend.com.evil.com", false},
		{"configured excludes same origin", []string{"https://frontend.com"}, "http://example.com", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := NewServer(NewHub(), tc.allowed...)
			r := httptest.NewRequest(http.MethodGet, "http://example.com/ws", nil)
			if tc.origin != "" {
				r.Header.Set("Origin", tc.origin)
			}
			if got := s.upgrader.CheckOrigin(r); got != tc.want {
				t.Fatalf("origin allowed=%v want=%v", got, tc.want)
			}
		})
	}
}

func TestURLTokensDisabledAndRedacted(t *testing.T) {
	s := NewServer(NewHub())
	s.RequireAuth = true
	r := httptest.NewRequest(http.MethodGet, "http://example.com/ws?token=sensitive&event=1", nil)
	w := httptest.NewRecorder()
	s.Handle(w, r)
	if w.Code != http.StatusUnauthorized || strings.Contains(r.RequestURI, "sensitive") || r.URL.Query().Get("token") != "" || r.URL.Query().Get("event") != "1" {
		t.Fatalf("status=%d uri=%s", w.Code, r.RequestURI)
	}
}

func TestAuthenticatedUpgradeWithHeaderAndBrowserSubprotocol(t *testing.T) {
	auth.Init("test-secret")
	token, err := auth.GenerateToken("user-1", "a@b.com")
	if err != nil {
		t.Fatal(err)
	}
	s := NewServer(NewHub(), "https://frontend.com")
	s.RequireAuth = true
	ts := httptest.NewServer(http.HandlerFunc(s.Handle))
	defer ts.Close()
	for _, browser := range []bool{false, true} {
		dialer := ws.Dialer{}
		header := http.Header{}
		header.Set("Origin", "https://frontend.com")
		if browser {
			dialer.Subprotocols = []string{"bearer", token}
		} else {
			header.Set("Authorization", "Bearer "+token)
		}
		conn, _, err := dialer.Dial("ws"+strings.TrimPrefix(ts.URL, "http"), header)
		if err != nil {
			t.Fatalf("browser=%v: %v", browser, err)
		}
		if browser && conn.Subprotocol() != "bearer" {
			t.Fatal("server must echo bearer, without the JWT")
		}
		conn.Close()
	}
	_, resp, err := ws.DefaultDialer.Dial("ws"+strings.TrimPrefix(ts.URL, "http"), http.Header{"Origin": []string{"https://evil.com"}, "Authorization": []string{"Bearer " + token}})
	if err == nil || resp == nil || resp.StatusCode != http.StatusForbidden {
		t.Fatalf("cross-origin upgrade accepted: %v %+v", err, resp)
	}
	if resp != nil {
		resp.Body.Close()
	}
}
