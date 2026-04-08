package daemon

import "testing"

func TestStrikethroughTaskTitle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		title string
		want  string
	}{
		{
			name:  "wraps trimmed titles with dim and strikethrough",
			title: "  Queue merge queue  ",
			want:  "\x1b[2m\x1b[9mQueue merge queue\x1b[29m\x1b[22m",
		},
		{
			name:  "returns empty when title is blank",
			title: " \t ",
			want:  "",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := strikethroughTaskTitle(tt.title); got != tt.want {
				t.Fatalf("strikethroughTaskTitle() = %q, want %q", got, tt.want)
			}
		})
	}
}
