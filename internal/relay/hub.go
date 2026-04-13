package relay

import (
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	defaultReplayCapacity = 1000
	defaultSendQueueSize  = 1024
	writeTimeout          = 5 * time.Second
)

type Hub struct {
	mu            sync.Mutex
	now           func() time.Time
	nextID        uint64
	replay        ringBuffer
	sendQueueSize int
	clients       map[*client]struct{}
}

func NewHub(size int, now func() time.Time) *Hub {
	if size <= 0 {
		size = defaultReplayCapacity
	}
	if now == nil {
		now = time.Now
	}

	return &Hub{
		now:           now,
		replay:        ringBuffer{max: size},
		sendQueueSize: queueSizeForReplay(size),
		clients:       make(map[*client]struct{}),
	}
}

func (h *Hub) Publish(event Envelope) Envelope {
	h.mu.Lock()
	h.nextID++
	event.ID = h.nextID
	if event.Timestamp.IsZero() {
		event.Timestamp = h.now().UTC()
	}
	h.replay.append(event)

	var slowClients []*client
	for client := range h.clients {
		select {
		case client.send <- event:
		default:
			slowClients = append(slowClients, client)
		}
	}
	h.mu.Unlock()

	for _, client := range slowClients {
		client.close()
	}

	return event
}

func (h *Hub) Attach(conn *websocket.Conn, lastEventID uint64) {
	client := &client{
		conn: conn,
		hub:  h,
		send: make(chan Envelope, h.sendQueueSize),
	}

	h.mu.Lock()
	for _, event := range h.replay.after(lastEventID) {
		client.send <- event
	}
	h.clients[client] = struct{}{}
	h.mu.Unlock()

	go client.writeLoop()
	go client.readLoop()
}

type client struct {
	closeOnce sync.Once
	conn      *websocket.Conn
	hub       *Hub
	send      chan Envelope
}

func (c *client) writeLoop() {
	for event := range c.send {
		if err := c.conn.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
			c.close()
			return
		}
		if err := c.conn.WriteJSON(event); err != nil {
			c.close()
			return
		}
	}
}

func (c *client) readLoop() {
	defer c.close()

	for {
		if _, _, err := c.conn.ReadMessage(); err != nil {
			return
		}
	}
}

func (c *client) close() {
	c.closeOnce.Do(func() {
		c.hub.mu.Lock()
		delete(c.hub.clients, c)
		c.hub.mu.Unlock()

		close(c.send)
		_ = c.conn.Close()
	})
}

type ringBuffer struct {
	max    int
	events []Envelope
}

func (r *ringBuffer) append(event Envelope) {
	if r.max <= 0 {
		return
	}
	if len(r.events) == r.max {
		copy(r.events, r.events[1:])
		r.events[len(r.events)-1] = event
		return
	}
	r.events = append(r.events, event)
}

func (r *ringBuffer) after(lastEventID uint64) []Envelope {
	if len(r.events) == 0 {
		return nil
	}

	start := 0
	for start < len(r.events) && r.events[start].ID <= lastEventID {
		start++
	}
	out := make([]Envelope, len(r.events)-start)
	copy(out, r.events[start:])
	return out
}

func queueSizeForReplay(replaySize int) int {
	if replaySize+24 > defaultSendQueueSize {
		return replaySize + 24
	}
	return defaultSendQueueSize
}
