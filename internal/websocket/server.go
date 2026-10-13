package websocket

import (
	"net/http"
	"net/url"
	"strings"

	"predix/pkg/auth"

	"github.com/gorilla/websocket"
)

type Server struct {
	Hub *Hub

	// RequireAuth demands a JWT via Bearer header or the bearer subprotocol.
	RequireAuth bool
	// Legacy URL tokens require explicit opt-in because proxies may log URLs.
	AllowQueryToken bool

	upgrader websocket.Upgrader
}

func NewServer(hub *Hub, allowedOrigins ...string) *Server {
	origins := make(map[string]bool, len(allowedOrigins))
	for _, origin := range allowedOrigins {
		origins[origin] = true
	}
	return &Server{
		Hub: hub,

		upgrader: websocket.Upgrader{

			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			Subprotocols:    []string{"bearer"},

			CheckOrigin: func(
				r *http.Request,
			) bool {
				values := r.Header.Values("Origin")
				if len(values) == 0 {
					return true
				}
				if len(values) != 1 {
					return false
				}
				origin := values[0]
				if origins[origin] {
					return true
				}
				if len(origins) > 0 {
					return false
				}
				u, err := url.Parse(origin)
				scheme := "http"
				if r.TLS != nil {
					scheme = "https"
				}
				return err == nil && u.Scheme == scheme && u.Host == r.Host && u.User == nil && u.Path == "" && u.RawQuery == "" && u.Fragment == ""
			},
		},
	}
}

func (s *Server) Handle(
	w http.ResponseWriter,
	r *http.Request,
) {
	// Sanitize application-level URL logging. Upstream access logs still need
	// redaction when legacy query tokens are explicitly enabled.
	query := r.URL.Query()
	queryToken := query.Get("token")
	query.Del("token")
	r.URL.RawQuery = query.Encode()
	r.RequestURI = r.URL.RequestURI()

	if s.RequireAuth {
		var token string
		if queryToken != "" && !s.AllowQueryToken {
			http.Error(w, "URL tokens are disabled", http.StatusUnauthorized)
			return
		}
		if header := r.Header.Get("Authorization"); header != "" {
			parts := strings.Fields(header)
			if len(parts) == 2 && strings.EqualFold(parts[0], "Bearer") {
				token = parts[1]
			}
		} else {
			protocols := websocket.Subprotocols(r)
			if len(protocols) == 2 && protocols[0] == "bearer" {
				token = protocols[1]
			}
			if token == "" && s.AllowQueryToken {
				token = queryToken
			}
		}

		if token == "" {
			http.Error(w, "missing token", http.StatusUnauthorized)
			return
		}

		if _, err := auth.ValidateToken(token); err != nil {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
	}

	conn, err := s.upgrader.Upgrade(
		w,
		r,
		nil,
	)

	if err != nil {
		return
	}

	client := NewClient(
		s.Hub,
		conn,
	)

	s.Hub.Register(client)

	go client.WritePump()
	go client.ReadPump()
}
