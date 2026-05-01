package daemon

import "testing"

func TestCodexOutputMatchesUpdatePermissionError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		output string
		want   bool
	}{
		{
			name:   "global install failed",
			output: "Codex global npm install failed while checking for updates",
			want:   true,
		},
		{
			name:   "global install eacces",
			output: "global npm install returned EACCES",
			want:   true,
		},
		{
			name:   "global install permission",
			output: "global npm install requires permission",
			want:   true,
		},
		{
			name:   "usr lib node modules eacces",
			output: "npm ERR! code EACCES\nnpm ERR! path /usr/lib/node_modules/@openai",
			want:   true,
		},
		{
			name:   "usr lib node modules permission denied",
			output: "Error: permission denied, mkdir '/usr/lib/node_modules/@openai'",
			want:   true,
		},
		{
			name:   "usr lib node modules operation rejected",
			output: "npm ERR! The operation was rejected by your operating system\nnpm ERR! path /usr/lib/node_modules",
			want:   true,
		},
		{
			name:   "empty output",
			output: "",
			want:   false,
		},
		{
			name:   "whitespace output",
			output: " \n\t ",
			want:   false,
		},
		{
			name:   "global install without failure",
			output: "global npm install completed",
			want:   false,
		},
		{
			name:   "permission without global install or node modules",
			output: "permission denied while reading config",
			want:   false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := codexOutputMatchesUpdatePermissionError(tt.output); got != tt.want {
				t.Fatalf("codexOutputMatchesUpdatePermissionError() = %v, want %v", got, tt.want)
			}
		})
	}
}
