package daemon

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

func issueCommentWatermark(comments []prComment) string {
	if len(comments) == 0 {
		return ""
	}

	signatures := make([]string, 0, len(comments))
	for _, comment := range comments {
		signatures = append(signatures, issueCommentSignature(comment))
	}
	return strings.Join(signatures, "\n")
}

func changedIssueComments(comments []prComment, previousWatermark string) []prComment {
	if len(comments) == 0 {
		return nil
	}

	seen := make(map[string]struct{})
	for _, signature := range strings.Split(previousWatermark, "\n") {
		signature = strings.TrimSpace(signature)
		if signature == "" {
			continue
		}
		seen[signature] = struct{}{}
	}

	changed := make([]prComment, 0, len(comments))
	for _, comment := range comments {
		signature := issueCommentSignature(comment)
		if _, ok := seen[signature]; ok {
			continue
		}
		changed = append(changed, comment)
	}
	return changed
}

func issueCommentSignature(comment prComment) string {
	sum := sha256.Sum256([]byte(comment.Body))
	bodyHash := hex.EncodeToString(sum[:])
	commentID := strings.TrimSpace(comment.ID)
	if commentID == "" {
		return bodyHash
	}
	return commentID + ":" + bodyHash
}
