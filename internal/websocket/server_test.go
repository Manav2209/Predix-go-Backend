package websocket

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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
