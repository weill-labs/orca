package daemon

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"
)

const reviewThreadSignaturePrefix = "thread:"

func reviewFeedbackWatermark(comments []prComment, threads []prReviewThread) string {
	signatures := make([]string, 0, len(comments)+len(threads))
	for _, comment := range comments {
		signatures = append(signatures, issueCommentSignature(comment))
	}
	for _, thread := range threads {
		signature := reviewThreadSignature(thread)
		if signature == "" {
			continue
		}
		signatures = append(signatures, signature)
	}
	sort.Strings(signatures)
	return strings.Join(signatures, "\n")
}

func changedIssueComments(comments []prComment, previousWatermark string) []prComment {
	if len(comments) == 0 {
		return nil
	}

	seen := watermarkSet(previousWatermark)
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

func changedReviewThreads(threads []prReviewThread, previousWatermark string) []prReviewThread {
	if len(threads) == 0 {
		return nil
	}

	seen := watermarkSet(previousWatermark)
	changed := make([]prReviewThread, 0, len(threads))
	for _, thread := range threads {
		signature := reviewThreadSignature(thread)
		if signature == "" {
			continue
		}
		if _, ok := seen[signature]; ok {
			continue
		}
		changed = append(changed, thread)
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

func reviewThreadSignature(thread prReviewThread) string {
	if thread.IsResolved {
		return ""
	}

	comment, _ := latestReviewThreadComment(thread)
	threadID := strings.TrimSpace(thread.ID)
	if threadID == "" {
		threadID = fallbackReviewThreadID(thread)
	}
	if threadID == "" {
		return ""
	}

	commentID := strings.TrimSpace(comment.ID)
	if commentID == "" {
		commentID = comment.CreatedAt.UTC().Format("2006-01-02T15:04:05.999999999Z07:00")
	}
	sum := sha256.Sum256([]byte(comment.Body))
	bodyHash := hex.EncodeToString(sum[:])
	return reviewThreadSignaturePrefix + threadID + ":" + commentID + ":" + bodyHash
}

func fallbackReviewThreadID(thread prReviewThread) string {
	comment, ok := latestReviewThreadComment(thread)
	if !ok {
		return strings.TrimSpace(thread.Path)
	}
	sum := sha256.Sum256([]byte(strings.TrimSpace(thread.Path) + "\n" + comment.Body))
	return hex.EncodeToString(sum[:])
}

func watermarkSet(watermark string) map[string]struct{} {
	seen := make(map[string]struct{})
	for _, signature := range strings.Split(watermark, "\n") {
		signature = strings.TrimSpace(signature)
		if signature == "" {
			continue
		}
		seen[signature] = struct{}{}
	}
	return seen
}
