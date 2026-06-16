#!/bin/bash
# Shell test for scripts/oos-daily-launchd-runner.sh exit-code -> marker logic.
# Stubs the runner via OOS_RUNNER_CMD and isolates all state under a temp OOS_DATAROOM.
# No network, no real cycle, never touches the real cumulative dir.

WRAPPER="$(cd "$(dirname "$0")/.." && pwd)/scripts/oos-daily-launchd-runner.sh"
fails=0
present() { if [ -f "$1" ]; then echo "  ok: $2 present"; else echo "  FAIL: $2 MISSING"; fails=$((fails+1)); fi; }
absent()  { if [ ! -f "$1" ]; then echo "  ok: $2 absent"; else echo "  FAIL: $2 PRESENT unexpectedly"; fails=$((fails+1)); fi; }
eq()      { if [ "$1" = "$2" ]; then echo "  ok: $3"; else echo "  FAIL: $3 (got '$1' want '$2')"; fails=$((fails+1)); fi; }

mk() { local d; d="$(mktemp -d)"; printf 'dummy\n' > "$d/token"; echo "$d"; }
run() { # $1=dir  $2=runner-cmd  $3=optional wrapper arg ; sets RC
  OOS_DATAROOM="$1" OOS_DASHBOARD_TOKEN_FILE="$1/token" OOS_RUNNER_CMD="$2" bash "$WRAPPER" "${3:-}" >/dev/null 2>&1
  RC=$?
}

echo "A: runner exit 0 -> no markers, wrapper exit 0"
A="$(mk)"; printf '#!/bin/bash\nexit 0\n' > "$A/s.sh"
run "$A" "bash $A/s.sh"; eq "$RC" 0 "wrapper exit 0"
absent "$A/oos-runner-state/LOOKPOINT_READY_N50_AUDIT_REQUIRED" "HALT marker"
absent "$A/oos-runner-state/FAIL_CLOSED_BLOCKER" "BLOCK marker"
rm -rf "$A"

echo "B: runner exit 10 -> HALT marker + subsequent run skips"
B="$(mk)"; printf '#!/bin/bash\nexit 10\n' > "$B/s.sh"
run "$B" "bash $B/s.sh"; eq "$RC" 0 "wrapper exit 0"
present "$B/oos-runner-state/LOOKPOINT_READY_N50_AUDIT_REQUIRED" "HALT marker"
printf '#!/bin/bash\ntouch %s/RAN\nexit 0\n' "$B" > "$B/s2.sh"
run "$B" "bash $B/s2.sh"
absent "$B/RAN" "HALT marker prevents re-run"
rm -rf "$B"

echo "C: runner exit 2 -> BLOCK marker"
C="$(mk)"; printf '#!/bin/bash\nexit 2\n' > "$C/s.sh"
run "$C" "bash $C/s.sh"; eq "$RC" 0 "wrapper exit 0"
present "$C/oos-runner-state/FAIL_CLOSED_BLOCKER" "BLOCK marker"
rm -rf "$C"

echo "D: --check smoke -> no markers"
D="$(mk)"; printf '#!/bin/bash\nexit 0\n' > "$D/s.sh"
run "$D" "bash $D/s.sh" "--check"; eq "$RC" 0 "smoke exit 0"
absent "$D/oos-runner-state/LOOKPOINT_READY_N50_AUDIT_REQUIRED" "smoke wrote no HALT marker"
rm -rf "$D"

if [ "$fails" = 0 ]; then echo "ALL PASS"; exit 0; else echo "$fails FAILURE(S)"; exit 1; fi
