package daemon

import "testing"

func TestNormalizePromptForDelivery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		prompt  string
		want    string
		wantErr string
	}{
		{
			name:   "keeps single line prompts unchanged",
			prompt: "Implement LAB-1397",
			want:   "Implement LAB-1397",
		},
		{
			name:   "flattens multiline prompts",
			prompt: "Implement LAB-1397\n\nRun the targeted tests\nand open a PR.\n",
			want:   "Implement LAB-1397 Run the targeted tests and open a PR.",
		},
		{
			name:    "rejects empty prompt after flattening",
			prompt:  "\n  \n",
			wantErr: "prompt is empty",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := normalizePromptForDelivery(tt.prompt)
			if tt.wantErr != "" {
				if err == nil || err.Error() != tt.wantErr {
					t.Fatalf("normalizePromptForDelivery() error = %v, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("normalizePromptForDelivery() error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("normalizePromptForDelivery() = %q, want %q", got, tt.want)
			}
		})
	}
}
