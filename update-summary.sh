#!/usr/bin/env bash
#
# SLURM submission header (effective when submitted with `sbatch`).
# Safe to ignore if launched directly by cron; the script still works.
#SBATCH -p compute
#SBATCH -n 25
#SBATCH --mem=50000M
#SBATCH --job-name=daily-report
#SBATCH --output=/bil/data/inventory/daily/logs/daily-report_%j.out
#SBATCH --error=/bil/data/inventory/daily/logs/daily-report_%j.err
#SBATCH --mail-type=FAIL

###############################################################################
# Daily reporting pipeline for @icaoberg
#
# What this script does
#  1) Activates a conda environment (base) for user icaoberg.
#  2) Runs a sequence of Python steps to build/update today.json.
#  3) Applies safe, idempotent JSON fixes using `jq` (not brittle sed).
#  4) Publishes today.json to /bil/data/inventory/daily/reports/.
#  5) Logs everything and exits on first error.
#
# Cron example (06:10 every day):
#  10 6 * * * /bin/bash -lc '/bil/data/inventory/daily/run_daily.sh >> /bil/data/inventory/daily/logs/cron.log 2>&1'
#
# Requirements: bash, conda, jq, python; the Python scripts live next to this file.
###############################################################################

set -Eeuo pipefail

############################
# Config (edit as needed)  #
############################
ALLOWED_USER="icaoberg"
CONDA_SH="/bil/users/${ALLOWED_USER}/miniconda3/etc/profile.d/conda.sh"
CONDA_ENV="base"

# Resolve repository dir to the directory containing this script.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="${SCRIPT_DIR}"

# Working/output files
TODAY_JSON="${REPO_DIR}/today.json"
PUBLISH_DIR="/bil/data/inventory/daily/reports"

# Logs (used when invoked directly; SLURM has its own via #SBATCH)
LOG_DIR="/bil/data/inventory/daily/logs"
mkdir -p "${LOG_DIR}"

# Simple lock to prevent overlapping runs
LOCK_DIR="/tmp/daily-report.lock"

############################
# Helpers                  #
############################
log()  { printf '[%(%F %T)T] %s\n' -1 "$*"; }
fail() { log "ERROR: $*"; exit 1; }

cleanup() {
  # runs on exit or error
  if [[ -d "${LOCK_DIR}" ]]; then rm -rf "${LOCK_DIR}"; fi
}
trap cleanup EXIT

take_lock() {
  if ! mkdir "${LOCK_DIR}" 2>/dev/null; then
    fail "Another instance appears to be running (lock: ${LOCK_DIR})."
  fi
}

require_user() {
  if [[ "${USER:-}" != "${ALLOWED_USER}" ]]; then
    fail "Only user ${ALLOWED_USER} can run this script (current: ${USER:-unknown})."
  fi
}

# OPTION A: Temporarily disable `-u` for conda activation (handles activate+internal deactivate hooks)
activate_conda() {
  if [[ -f "${CONDA_SH}" ]]; then
    set +u
    # shellcheck disable=SC1090
    . "${CONDA_SH}"
    conda activate "${CONDA_ENV}"
    set -u
  else
    fail "Conda activation script not found at ${CONDA_SH}"
  fi
}

run_py() {
  local step="$1"
  log "▶ Running: ${step}"
  python -u "${REPO_DIR}/${step}"
  log "✔ Done: ${step}"
}

apply_json_fixes() {
  if [[ ! -f "${TODAY_JSON}" ]]; then
    fail "Expected ${TODAY_JSON} to exist before JSON fixes."
  fi

  # Validate JSON before modifying
  if ! jq empty "${TODAY_JSON}" >/dev/null 2>&1; then
    fail "Invalid JSON in ${TODAY_JSON}"
  fi

  # Apply idempotent, safe transformations.
  tmpfile="$(mktemp "${TODAY_JSON}.XXXX")"
  jq '
    # Fix specific name with trailing space
    (.. | strings) |= gsub("Allen Institute for Brain Science "; "Allen Institute for Brain Science")
    |
    # Ensure "award_number" is set to "Unavailable" when null or missing
    if has("award_number") then
      .award_number = (if .award_number == null or .award_number == "" then "Unavailable" else .award_number end)
    else
      . + {award_number: "Unavailable"}
    end
    |
    # Normalize species capitalization if field exists
    (if has("species") then
       .species |= (
          if . == "Mouse" then "mouse"
          elif . == "Human" then "human"
          elif . == "Marmoset" then "marmoset"
          elif . == "other" then "Other"
          else .
          end
       )
     else .
     end)
  ' "${TODAY_JSON}" > "${tmpfile}"

  mv -f "${tmpfile}" "${TODAY_JSON}"
  log "✔ JSON fixes applied to ${TODAY_JSON}"
}

publish() {
  mkdir -p "${PUBLISH_DIR}"
  local target="${PUBLISH_DIR}/today.json"

  # Validate again before publishing
  jq empty "${TODAY_JSON}" >/dev/null 2>&1 || fail "Refusing to publish invalid JSON."

  # Replace atomically
  if [[ -f "${target}" ]]; then
    rm -f "${target}"
  fi
  cp -f "${TODAY_JSON}" "${PUBLISH_DIR}/"
  log "✔ Published ${TODAY_JSON} → ${PUBLISH_DIR}/"
}

############################
# Main                     #
############################
main() {
  take_lock
  require_user

  cd "${REPO_DIR}"

  log "Starting daily report pipeline as ${USER} in ${REPO_DIR}"
  activate_conda
  log "Conda env: ${CONDA_ENV} ($(python -V 2>&1))"

  # Run steps (fail-fast on first error)
  run_py "generate_daily_report.py"
  run_py "update-summary.py"
  run_py "update-statistics.py"
  run_py "compute_score.py"
  run_py "convert_to_json.py"
  # run_py "update-missing-fields.py"   # enable when ready

  apply_json_fixes
  publish

  log "All done 🎉"
}

main "$@"