package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/weill-labs/orca/internal/mailbox"
)

// SQLiteStore implements mailbox.MailStore. These methods back the "orca mail"
// worker<->lead channel; see docs/specs/worker-lead-messaging.md.

const mailSelectColumns = `id, project, thread_id, sender, recipient, body, importance, kind, ask_id, reply_body, read_at, created_at`

// SendMail persists a note or ask and returns the stored message.
func (s *SQLiteStore) SendMail(ctx context.Context, msg mailbox.Send) (mailbox.Message, error) {
	stored := mailbox.Message{
		Project:    msg.Project,
		ThreadID:   msg.ThreadID,
		Sender:     msg.Sender,
		Recipient:  msg.Recipient,
		Body:       msg.Body,
		Importance: defaultImportance(msg.Importance),
		Kind:       defaultKind(msg.Kind),
		CreatedAt:  s.now(),
	}
	return s.insertMail(ctx, stored)
}

// Inbox returns messages addressed to recipient in the project, newest first.
func (s *SQLiteStore) Inbox(ctx context.Context, project, recipient string, unreadOnly bool) ([]mailbox.Message, error) {
	query := `SELECT ` + mailSelectColumns + `
		FROM mail
		WHERE project = ? AND recipient = ?`
	if unreadOnly {
		query += ` AND read_at IS NULL`
	}
	query += ` ORDER BY id DESC`

	rows, err := s.db.QueryContext(ctx, query, project, recipient)
	if err != nil {
		return nil, fmt.Errorf("inbox: %w", err)
	}
	defer rows.Close()

	messages := make([]mailbox.Message, 0)
	for rows.Next() {
		msg, err := scanMailRow(rows)
		if err != nil {
			return nil, err
		}
		messages = append(messages, msg)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate inbox: %w", err)
	}
	return messages, nil
}

// MarkMailRead marks a single message read.
func (s *SQLiteStore) MarkMailRead(ctx context.Context, project string, id int64, readAt time.Time) error {
	if readAt.IsZero() {
		readAt = s.now()
	}
	result, err := s.db.ExecContext(ctx, `
		UPDATE mail SET read_at = ? WHERE project = ? AND id = ?
	`, formatTime(readAt), project, id)
	if err != nil {
		return fmt.Errorf("mark mail read: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("mark mail read rows affected: %w", err)
	}
	if affected == 0 {
		return ErrNotFound
	}
	return nil
}

// ReplyMail records a reply to an ask and stamps the ask's reply_body so a
// worker polling the ask sees the answer without a join.
func (s *SQLiteStore) ReplyMail(ctx context.Context, project string, askID int64, sender, body string) (mailbox.Message, error) {
	ask, err := s.lookupMail(ctx, project, askID)
	if err != nil {
		return mailbox.Message{}, err
	}

	reply, err := s.insertMail(ctx, mailbox.Message{
		Project:    project,
		ThreadID:   ask.ThreadID,
		Sender:     sender,
		Recipient:  ask.Sender,
		Body:       body,
		Importance: mailbox.ImportanceNormal,
		Kind:       mailbox.KindReply,
		AskID:      &askID,
		CreatedAt:  s.now(),
	})
	if err != nil {
		return mailbox.Message{}, err
	}

	if _, err := s.db.ExecContext(ctx, `
		UPDATE mail SET reply_body = ? WHERE project = ? AND id = ?
	`, body, project, askID); err != nil {
		return mailbox.Message{}, fmt.Errorf("reply mail update ask: %w", err)
	}
	return reply, nil
}

// insertMail writes a fully-populated message row and returns it with the
// assigned id. CreatedAt, Importance and Kind must already be set.
func (s *SQLiteStore) insertMail(ctx context.Context, msg mailbox.Message) (mailbox.Message, error) {
	var askID any
	if msg.AskID != nil {
		askID = *msg.AskID
	}
	result, err := s.db.ExecContext(ctx, `
		INSERT INTO mail(project, thread_id, sender, recipient, body, importance, kind, ask_id, reply_body, read_at, created_at)
		VALUES(?, ?, ?, ?, ?, ?, ?, ?, '', NULL, ?)
	`, msg.Project, msg.ThreadID, msg.Sender, msg.Recipient, msg.Body,
		string(msg.Importance), string(msg.Kind), askID, formatTime(msg.CreatedAt))
	if err != nil {
		return mailbox.Message{}, fmt.Errorf("insert mail: %w", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return mailbox.Message{}, fmt.Errorf("insert mail last insert id: %w", err)
	}
	msg.ID = id
	return msg, nil
}

func (s *SQLiteStore) lookupMail(ctx context.Context, project string, id int64) (mailbox.Message, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+mailSelectColumns+`
		FROM mail WHERE project = ? AND id = ?`, project, id)
	msg, err := scanMailRow(row)
	if errors.Is(err, sql.ErrNoRows) {
		return mailbox.Message{}, ErrNotFound
	}
	if err != nil {
		return mailbox.Message{}, err
	}
	return msg, nil
}

func scanMailRow(scanner rowScanner) (mailbox.Message, error) {
	var (
		msg        mailbox.Message
		importance string
		kind       string
		askID      sql.NullInt64
		readAt     sql.NullString
		createdAt  string
	)
	if err := scanner.Scan(
		&msg.ID, &msg.Project, &msg.ThreadID, &msg.Sender, &msg.Recipient,
		&msg.Body, &importance, &kind, &askID, &msg.ReplyBody, &readAt, &createdAt,
	); err != nil {
		return mailbox.Message{}, err
	}
	msg.Importance = mailbox.Importance(importance)
	msg.Kind = mailbox.Kind(kind)
	if askID.Valid {
		value := askID.Int64
		msg.AskID = &value
	}
	if readAt.Valid && readAt.String != "" {
		readTime := parseTime(readAt.String)
		msg.ReadAt = &readTime
	}
	msg.CreatedAt = parseTime(createdAt)
	return msg, nil
}

func defaultImportance(i mailbox.Importance) mailbox.Importance {
	if i == "" {
		return mailbox.ImportanceNormal
	}
	return i
}

func defaultKind(k mailbox.Kind) mailbox.Kind {
	if k == "" {
		return mailbox.KindNote
	}
	return k
}
