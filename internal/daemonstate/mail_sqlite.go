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
	now := s.now()
	importance := msg.Importance
	if importance == "" {
		importance = mailbox.ImportanceNormal
	}
	kind := msg.Kind
	if kind == "" {
		kind = mailbox.KindNote
	}

	result, err := s.db.ExecContext(ctx, `
		INSERT INTO mail(project, thread_id, sender, recipient, body, importance, kind, ask_id, reply_body, read_at, created_at)
		VALUES(?, ?, ?, ?, ?, ?, ?, NULL, '', NULL, ?)
	`, msg.Project, msg.ThreadID, msg.Sender, msg.Recipient, msg.Body, string(importance), string(kind), formatTime(now))
	if err != nil {
		return mailbox.Message{}, fmt.Errorf("send mail: %w", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return mailbox.Message{}, fmt.Errorf("send mail last insert id: %w", err)
	}

	return mailbox.Message{
		ID:         id,
		Project:    msg.Project,
		ThreadID:   msg.ThreadID,
		Sender:     msg.Sender,
		Recipient:  msg.Recipient,
		Body:       msg.Body,
		Importance: importance,
		Kind:       kind,
		CreatedAt:  now,
	}, nil
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

// ReplyMail records a reply to an ask and stamps the ask's reply_body.
func (s *SQLiteStore) ReplyMail(ctx context.Context, project string, askID int64, sender, body string) (mailbox.Message, error) {
	ask, err := s.lookupMail(ctx, project, askID)
	if err != nil {
		return mailbox.Message{}, err
	}

	now := s.now()
	result, err := s.db.ExecContext(ctx, `
		INSERT INTO mail(project, thread_id, sender, recipient, body, importance, kind, ask_id, reply_body, read_at, created_at)
		VALUES(?, ?, ?, ?, ?, ?, ?, ?, '', NULL, ?)
	`, project, ask.ThreadID, sender, ask.Sender, body, string(mailbox.ImportanceNormal), string(mailbox.KindReply), askID, formatTime(now))
	if err != nil {
		return mailbox.Message{}, fmt.Errorf("reply mail: %w", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return mailbox.Message{}, fmt.Errorf("reply mail last insert id: %w", err)
	}

	if _, err := s.db.ExecContext(ctx, `
		UPDATE mail SET reply_body = ? WHERE project = ? AND id = ?
	`, body, project, askID); err != nil {
		return mailbox.Message{}, fmt.Errorf("reply mail update ask: %w", err)
	}

	askIDCopy := askID
	return mailbox.Message{
		ID:         id,
		Project:    project,
		ThreadID:   ask.ThreadID,
		Sender:     sender,
		Recipient:  ask.Sender,
		Body:       body,
		Importance: mailbox.ImportanceNormal,
		Kind:       mailbox.KindReply,
		AskID:      &askIDCopy,
		CreatedAt:  now,
	}, nil
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
