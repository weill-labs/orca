package daemon

import (
	"errors"
	"strings"
)

func normalizePromptForDelivery(prompt string) (string, error) {
	trimmed := strings.TrimRight(prompt, "\r\n")
	if strings.ContainsAny(trimmed, "\r\n") {
		lines := strings.FieldsFunc(trimmed, func(r rune) bool {
			return r == '\r' || r == '\n'
		})
		flattened := lines[:0]
		for _, line := range lines {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			flattened = append(flattened, line)
		}
		trimmed = strings.Join(flattened, " ")
	}
	if trimmed == "" {
		return "", errors.New("prompt is empty")
	}
	return trimmed, nil
}
