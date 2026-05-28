package daemon

import (
	"reflect"
	"testing"
)

func TestAssignTypesIncludeNotifyPaneField(t *testing.T) {
	t.Parallel()

	assignRequestField, ok := reflect.TypeOf(AssignRequest{}).FieldByName("NotifyPane")
	if !ok {
		t.Fatal("AssignRequest missing NotifyPane field")
	}
	if got, want := assignRequestField.Type.Kind(), reflect.String; got != want {
		t.Fatalf("AssignRequest.NotifyPane kind = %v, want %v", got, want)
	}
	if got := assignRequestField.Tag.Get("json"); got != "" {
		t.Fatalf("AssignRequest.NotifyPane json tag = %q, want empty", got)
	}

	assignRPCField, ok := reflect.TypeOf(assignRPCParams{}).FieldByName("NotifyPane")
	if !ok {
		t.Fatal("assignRPCParams missing NotifyPane field")
	}
	if got, want := assignRPCField.Tag.Get("json"), "notify_pane"; got != want {
		t.Fatalf("assignRPCParams.NotifyPane json tag = %q, want %q", got, want)
	}
}
