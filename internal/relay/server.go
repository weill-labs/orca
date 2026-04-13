package relay

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/websocket"
)

const maxWebhookPayloadSize = 1 << 20

type ServerOptions struct {
	Now         func() time.Time
	ReplayLimit int
	Upgrader    websocket.Upgrader
}

type Server struct {
	cfg      Config
	hub      *Hub
	now      func() time.Time
	upgrader websocket.Upgrader
}

func NewServer(cfg Config, options ServerOptions) *Server {
	now := options.Now
	if now == nil {
		now = time.Now
	}

	upgrader := options.Upgrader
	if upgrader.CheckOrigin == nil {
		upgrader.CheckOrigin = func(*http.Request) bool { return true }
	}

	return &Server{
		cfg:      cfg,
		hub:      NewHub(options.ReplayLimit, now),
		now:      now,
		upgrader: upgrader,
	}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/webhook", s.handleWebhook)
	mux.HandleFunc("/ws", s.handleWebsocket)
	return mux
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true})
}

func (s *Server) handleWebhook(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxWebhookPayloadSize))
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			http.Error(w, "payload too large", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "read webhook payload", http.StatusBadRequest)
		return
	}

	if !VerifyGitHubSignature(s.cfg.WebhookSecret, body, r.Header.Get("X-Hub-Signature-256")) {
		http.Error(w, "invalid webhook signature", http.StatusUnauthorized)
		return
	}

	eventType := r.Header.Get("X-GitHub-Event")
	if eventType == "" {
		http.Error(w, "missing X-GitHub-Event header", http.StatusBadRequest)
		return
	}

	event, ok, err := NormalizeWebhookEvent(eventType, body, s.now().UTC())
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if ok {
		event = s.hub.Publish(event)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	if ok {
		_ = json.NewEncoder(w).Encode(event)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ignored"})
}

func (s *Server) handleWebsocket(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if r.URL.Query().Get("token") != s.cfg.RelayToken {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	lastEventID, err := parseLastEventID(r.URL.Query().Get("last_event_id"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		http.Error(w, fmt.Sprintf("upgrade websocket: %v", err), http.StatusBadRequest)
		return
	}

	s.hub.Attach(conn, lastEventID)
}

func parseLastEventID(raw string) (uint64, error) {
	if raw == "" {
		return 0, nil
	}

	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid last_event_id")
	}
	return value, nil
}
