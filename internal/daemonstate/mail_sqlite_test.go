package state

import "testing"

// SQLite wrappers for the backend-agnostic mail contract tests. The Postgres
// equivalents land with the Postgres mail store (orca-y5r.10.2) by calling the
// same testMail* functions with newPostgresContractHarness.

func TestSQLiteMailSendAndInbox(t *testing.T) {
	t.Parallel()
	testMailSendAndInbox(t, newSQLiteContractHarness(t))
}

func TestSQLiteMailInboxScopingAndOrder(t *testing.T) {
	t.Parallel()
	testMailInboxScopingAndOrder(t, newSQLiteContractHarness(t))
}

func TestSQLiteMailMarkRead(t *testing.T) {
	t.Parallel()
	testMailMarkRead(t, newSQLiteContractHarness(t))
}

func TestSQLiteMailReply(t *testing.T) {
	t.Parallel()
	testMailReply(t, newSQLiteContractHarness(t))
}
