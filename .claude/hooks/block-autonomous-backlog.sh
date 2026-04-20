#!/bin/bash
# PreToolUse hook: block send-keys commands that tell workers to autonomously
# pick up work from the backlog. This mistake has occurred 4+ times across
# sessions — mechanical enforcement is needed.
#
# Exit 2 = block the tool call and send feedback to Claude.

input=$(cat)
command=$(echo "$input" | jq -r '.tool_input.command // empty' 2>/dev/null)
backlog_pickup_pattern='\<pick up\>[^.!?]{0,40}\<(work|issue|task|ticket)\>|\<from\>[^.!?]{0,40}\<(backlog|queue|linear)\>|\<new work\>|\<next (issue|task|ticket)\>|\<find\>[^.!?]{0,40}\<(issue|task|work)\>[^.!?]{0,40}\<(backlog|queue|linear)\>'

# Only check amux send-keys and amux type-keys commands
if ! echo "$command" | grep -qE 'amux (send-keys|type-keys)'; then
    exit 0
fi

# Block commands that tell workers to pick up work autonomously
if echo "$command" | grep -qiE "$backlog_pickup_pattern"; then
    echo "BLOCKED: Do not tell workers to autonomously pick up work from the backlog. Only assign specific, user-approved issues. Ask the user which issue to assign." >&2
    exit 2
fi

exit 0
