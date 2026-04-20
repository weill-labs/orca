#!/usr/bin/env bash
set -euo pipefail

hooks_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
backlog_hook="${hooks_dir}/block-autonomous-backlog.sh"
exit_hook="${hooks_dir}/block-exit-without-postmortem.sh"

fail() {
	echo "FAIL: $*" >&2
	exit 1
}

command_payload() {
	local command="$1"

	jq -n --arg command "$command" '{tool_input: {command: $command}}'
}

assert_case() {
	local name="$1"
	local expectation="$2"
	local hook="$3"
	local command="$4"
	local stderr_file
	local status

	stderr_file="$(mktemp)"
	set +e
	command_payload "$command" | "$hook" >/dev/null 2>"${stderr_file}"
	status=$?
	set -e

	case "$expectation" in
		block)
			[ "$status" -eq 2 ] || fail "${name}: status=${status}, want 2"
			grep -q '^BLOCKED:' "${stderr_file}" || fail "${name}: missing BLOCKED message"
			;;
		allow)
			[ "$status" -eq 0 ] || fail "${name}: status=${status}, want 0"
			;;
		*)
			fail "${name}: unknown expectation ${expectation}"
			;;
	esac

	rm -f "${stderr_file}"
}

backlog_cases() {
	assert_case "block backlog pickup" block "${backlog_hook}" "amux send-keys pane-1 'pick up a task from the backlog'"
	assert_case "block next issue queue" block "${backlog_hook}" "amux send-keys pane-1 'grab the next issue off the queue'"
	assert_case "block linear backlog search" block "${backlog_hook}" "amux send-keys pane-1 'find a task in the linear backlog'"
	assert_case "block new work queue" block "${backlog_hook}" "amux send-keys pane-1 'start new work from the queue'"

	assert_case "allow actor guidance with queue filename" allow "${backlog_hook}" "amux send-keys pane-1 'consider moving from static function to per-task actor; see merge_queue_actor.go for the pattern'"
	assert_case "allow distant queueing mention" allow "${backlog_hook}" "amux send-keys pane-1 'reconstruct from the Postgres row when actor dies. Cross-cutting concerns like rate-limit queueing live elsewhere.'"
	assert_case "allow existing task filenames" allow "${backlog_hook}" "amux send-keys pane-1 'look at task_monitor_actor.go and prpoll.go for the existing patterns'"
	assert_case "allow non-amux commands" allow "${backlog_hook}" "printf 'pick up a task from the backlog'"
}

exit_cases() {
	assert_case "block slash exit" block "${exit_hook}" "amux send-keys pane-2 '/exit'"
	assert_case "block quoted slash exit" block "${exit_hook}" "amux type-keys pane-2 \"/exit\""
	assert_case "allow exitcode token" allow "${exit_hook}" "amux send-keys pane-2 '/exitcode'"
	assert_case "allow non-amux exit" allow "${exit_hook}" "printf '/exit'"
}

backlog_cases
exit_cases

echo "ok"
