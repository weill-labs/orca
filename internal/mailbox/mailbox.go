// Package mailbox defines the structured worker<->lead messaging channel for
// orca ("orca mail"). It replaces the raw amux send-keys notify convention with
// a durable, pullable message store.
//
// The package is deliberately severable: it declares the message shape and the
// MailStore contract but imports nothing from the daemon task state machine. A
// state backend (SQLite, Postgres) satisfies MailStore from the outside, so the
// messaging feature could be lifted into its own repo later without untangling
// daemon internals.
//
// See docs/specs/worker-lead-messaging.md for the design and decision log.
package mailbox

import (
	"context"
	"time"
)

// Kind classifies a message. A note is fire-and-forget; an ask expects a reply;
// a reply answers an earlier ask.
type Kind string

const (
	KindNote  Kind = "note"
	KindAsk   Kind = "ask"
	KindReply Kind = "reply"
)

// Importance mirrors the levels used by the agent-mail tools so callers can
// flag urgency without a separate channel.
type Importance string

const (
	ImportanceLow    Importance = "low"
	ImportanceNormal Importance = "normal"
	ImportanceHigh   Importance = "high"
	ImportanceUrgent Importance = "urgent"
)

// Message is a single worker<->lead message. Rows are scoped by Project (like
// every other orca table) and threaded by ThreadID, which is the issue id.
type Message struct {
	ID         int64      `json:"id"`
	Project    string     `json:"project"`
	ThreadID   string     `json:"thread_id,omitempty"`
	Sender     string     `json:"sender"`
	Recipient  string     `json:"recipient"`
	Body       string     `json:"body"`
	Importance Importance `json:"importance,omitempty"`
	Kind       Kind       `json:"kind,omitempty"`
	// AskID is set on a reply: the id of the ask it answers.
	AskID *int64 `json:"ask_id,omitempty"`
	// ReplyBody is filled on an ask once a reply arrives, so a worker polling a
	// single ask row sees the answer without a join.
	ReplyBody string `json:"reply_body,omitempty"`
	// ReadAt is nil while the message is unread.
	ReadAt    *time.Time `json:"read_at,omitempty"`
	CreatedAt time.Time  `json:"created_at"`
}

// Send is the input for posting a message. The store assigns ID and CreatedAt.
type Send struct {
	Project    string
	ThreadID   string
	Sender     string
	Recipient  string
	Body       string
	Importance Importance
	Kind       Kind
}

// MailStore is the persistence contract for orca mail. A state backend
// implements it; callers depend only on this interface and Message.
type MailStore interface {
	// SendMail persists a note or ask and returns the stored message.
	SendMail(ctx context.Context, msg Send) (Message, error)
	// Inbox returns messages addressed to recipient in the project, newest
	// first. When unreadOnly is true, read messages are omitted.
	Inbox(ctx context.Context, project, recipient string, unreadOnly bool) ([]Message, error)
	// MarkMailRead marks a single message read. It returns ErrNotFound if no
	// message with that id exists in the project.
	MarkMailRead(ctx context.Context, project string, id int64, readAt time.Time) error
	// ReplyMail records a reply to an ask: it inserts a reply message and sets
	// the original ask's ReplyBody. It returns the inserted reply. ErrNotFound
	// if the ask does not exist in the project.
	ReplyMail(ctx context.Context, project string, askID int64, sender, body string) (Message, error)
}
