package websocket

import (
	"net/http"

	"predix/pkg/auth"

	"github.com/gorilla/websocket"
)

type Server struct {
	Hub *Hub

	// RequireAuth demands a valid JWT in the ?token= query parameter before
	// upgrading (P3.8).
	RequireAuth bool

	upgrader websocket.Upgrader
}

func NewServer(hub *Hub) *Server {

	return &Server{
		Hub: hub,

		upgrader: websocket.Upgrader{

			ReadBufferSize:  1024,
			WriteBufferSize: 1024,

			CheckOrigin: func(
				r *http.Request,
			) bool {
				// Development version.
				// Restrict this in production.
				return true
			},
		},
	}
}

func (s *Server) Handle(
	w http.ResponseWriter,
	r *http.Request,
) {

	if s.RequireAuth {
		token := r.URL.Query().Get("token")

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
