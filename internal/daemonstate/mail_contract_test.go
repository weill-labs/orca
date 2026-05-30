package state

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/weill-labs/orca/internal/mailbox"
)

// mailStore is the subset of a backend exercised by the mail contract tests.
// A backend that does not yet implement orca mail simply fails the type
// assertion and the test is skipped, so each backend can land independently.
func mailStoreOrSkip(t *testing.T, h storeContractHarness) mailbox.MailStore {
	t.Helper()
	ms, ok := h.store.(mailbox.MailStore)
	if !ok {
		t.Skip("backend does not implement mailbox.MailStore yet")
	}
	return ms
}

func testMailSendAndInbox(t *testing.T, h storeContractHarness) {
	t.Helper()
	ms := mailStoreOrSkip(t, h)
	ctx := context.Background()
	now := time.Date(2026, 5, 30, 10, 0, 0, 0, time.UTC)
	h.setNow(now)

	const project = "/repo"
	sent, err := ms.SendMail(ctx, mailbox.Send{
		Project:    project,
		ThreadID:   "LAB-456",
		Sender:     "LAB-456",
		Recipient:  "lead",
		Body:       "PR #123 is up",
		Importance: mailbox.ImportanceHigh,
		Kind:       mailbox.KindNote,
	})
	if err != nil {
		t.Fatalf("SendMail() error = %v", err)
	}
	if sent.ID == 0 {
		t.Fatalf("SendMail() returned id 0, want assigned id")
	}
	if !sent.CreatedAt.Equal(now) {
		t.Fatalf("SendMail() CreatedAt = %v, want %v", sent.CreatedAt, now)
	}
	if sent.ReadAt != nil {
		t.Fatalf("SendMail() ReadAt = %v, want nil (unread)", sent.ReadAt)
	}

	inbox, err := ms.Inbox(ctx, project, "lead", true)
	if err != nil {
		t.Fatalf("Inbox() error = %v", err)
	}
	if len(inbox) != 1 {
		t.Fatalf("Inbox() len = %d, want 1", len(inbox))
	}
	got := inbox[0]
	if got.ID != sent.ID {
		t.Fatalf("Inbox()[0].ID = %d, want %d", got.ID, sent.ID)
	}
	if got.Body != "PR #123 is up" {
		t.Fatalf("Inbox()[0].Body = %q, want %q", got.Body, "PR #123 is up")
	}
	if got.ThreadID != "LAB-456" {
		t.Fatalf("Inbox()[0].ThreadID = %q, want %q", got.ThreadID, "LAB-456")
	}
	if got.Importance != mailbox.ImportanceHigh {
		t.Fatalf("Inbox()[0].Importance = %q, want %q", got.Importance, mailbox.ImportanceHigh)
	}
	if got.Kind != mailbox.KindNote {
		t.Fatalf("Inbox()[0].Kind = %q, want %q", got.Kind, mailbox.KindNote)
	}
}

func testMailInboxScopingAndOrder(t *testing.T, h storeContractHarness) {
	t.Helper()
	ms := mailStoreOrSkip(t, h)
	ctx := context.Background()
	now := time.Date(2026, 5, 30, 10, 0, 0, 0, time.UTC)
	h.setNow(now)

	const project = "/repo"
	send := func(recipient, body string) mailbox.Message {
		now = now.Add(time.Minute)
		h.setNow(now)
		m, err := ms.SendMail(ctx, mailbox.Send{
			Project: project, Sender: "w", Recipient: recipient, Body: body, Kind: mailbox.KindNote,
		})
		if err != nil {
			t.Fatalf("SendMail(%q) error = %v", body, err)
		}
		return m
	}

	send("lead", "first")
	second := send("lead", "second")
	send("other", "not for lead")
	// A different project must not leak in.
	if _, err := ms.SendMail(ctx, mailbox.Send{
		Project: "/other-repo", Sender: "w", Recipient: "lead", Body: "wrong project", Kind: mailbox.KindNote,
	}); err != nil {
		t.Fatalf("SendMail(other project) error = %v", err)
	}

	inbox, err := ms.Inbox(ctx, project, "lead", false)
	if err != nil {
		t.Fatalf("Inbox() error = %v", err)
	}
	if len(inbox) != 2 {
		t.Fatalf("Inbox() len = %d, want 2 (project+recipient scoped)", len(inbox))
	}
	if inbox[0].ID != second.ID {
		t.Fatalf("Inbox()[0].ID = %d, want %d (newest first)", inbox[0].ID, second.ID)
	}
}

func testMailMarkRead(t *testing.T, h storeContractHarness) {
	t.Helper()
	ms := mailStoreOrSkip(t, h)
	ctx := context.Background()
	now := time.Date(2026, 5, 30, 10, 0, 0, 0, time.UTC)
	h.setNow(now)

	const project = "/repo"
	sent, err := ms.SendMail(ctx, mailbox.Send{
		Project: project, Sender: "w", Recipient: "lead", Body: "read me", Kind: mailbox.KindNote,
	})
	if err != nil {
		t.Fatalf("SendMail() error = %v", err)
	}

	readAt := now.Add(time.Hour)
	if err := ms.MarkMailRead(ctx, project, sent.ID, readAt); err != nil {
		t.Fatalf("MarkMailRead() error = %v", err)
	}

	unread, err := ms.Inbox(ctx, project, "lead", true)
	if err != nil {
		t.Fatalf("Inbox(unreadOnly) error = %v", err)
	}
	if len(unread) != 0 {
		t.Fatalf("Inbox(unreadOnly) len = %d, want 0 after MarkMailRead", len(unread))
	}

	all, err := ms.Inbox(ctx, project, "lead", false)
	if err != nil {
		t.Fatalf("Inbox(all) error = %v", err)
	}
	if len(all) != 1 {
		t.Fatalf("Inbox(all) len = %d, want 1", len(all))
	}
	if all[0].ReadAt == nil || !all[0].ReadAt.Equal(readAt) {
		t.Fatalf("Inbox(all)[0].ReadAt = %v, want %v", all[0].ReadAt, readAt)
	}

	if err := ms.MarkMailRead(ctx, project, 99999, readAt); !errors.Is(err, ErrNotFound) {
		t.Fatalf("MarkMailRead(missing) error = %v, want ErrNotFound", err)
	}
}

func testMailReply(t *testing.T, h storeContractHarness) {
	t.Helper()
	ms := mailStoreOrSkip(t, h)
	ctx := context.Background()
	now := time.Date(2026, 5, 30, 10, 0, 0, 0, time.UTC)
	h.setNow(now)

	const project = "/repo"
	ask, err := ms.SendMail(ctx, mailbox.Send{
		Project: project, ThreadID: "LAB-456", Sender: "LAB-456", Recipient: "lead",
		Body: "approach A or B?", Kind: mailbox.KindAsk,
	})
	if err != nil {
		t.Fatalf("SendMail(ask) error = %v", err)
	}

	now = now.Add(time.Minute)
	h.setNow(now)
	reply, err := ms.ReplyMail(ctx, project, ask.ID, "lead", "B")
	if err != nil {
		t.Fatalf("ReplyMail() error = %v", err)
	}
	if reply.Kind != mailbox.KindReply {
		t.Fatalf("ReplyMail() Kind = %q, want %q", reply.Kind, mailbox.KindReply)
	}
	if reply.AskID == nil || *reply.AskID != ask.ID {
		t.Fatalf("ReplyMail() AskID = %v, want %d", reply.AskID, ask.ID)
	}
	if reply.Recipient != "LAB-456" {
		t.Fatalf("ReplyMail() Recipient = %q, want %q (original ask sender)", reply.Recipient, "LAB-456")
	}

	// The original ask, fetched by the asker, now carries the answer inline.
	askerInbox, err := ms.Inbox(ctx, project, "LAB-456", false)
	if err != nil {
		t.Fatalf("Inbox(asker) error = %v", err)
	}
	if len(askerInbox) != 1 {
		t.Fatalf("Inbox(asker) len = %d, want 1 (the reply)", len(askerInbox))
	}
	if askerInbox[0].ReplyBody != "" && askerInbox[0].Body != "B" {
		t.Fatalf("asker inbox did not surface the reply: %+v", askerInbox[0])
	}

	if _, err := ms.ReplyMail(ctx, project, 99999, "lead", "x"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("ReplyMail(missing ask) error = %v, want ErrNotFound", err)
	}
}
