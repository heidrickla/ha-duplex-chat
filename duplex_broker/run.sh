#!/usr/bin/with-contenv sh
# Options reach the broker through the environment, not argv, so secrets stay out of the
# process list. /data persists across add-on updates.
# with-contenv: s6-overlay keeps the container environment, SUPERVISOR_TOKEN included, out of CMD otherwise.
set -eu

OPTS=/data/options.json
opt() { jq -r ".$1 // empty" "$OPTS"; }

DUPLEX_DATA_DIR=/data
DUPLEX_HOST=0.0.0.0
DUPLEX_PORT=8790
DUPLEX_USER_AUTH=ingress
DUPLEX_PAIRING_CODE="$(opt pairing_code)"
DUPLEX_ENROLLMENT_TOKEN="$(opt enrollment_token)"
DUPLEX_SERVICES="$(jq -c '.services // []' "$OPTS")"
DUPLEX_OWNER_NAME="$(opt owner_name)"
DUPLEX_WAKER_TOKEN="$(opt waker_token)"
DUPLEX_DEVICE_TOKEN_GRACE="$(opt device_token_grace)"
DUPLEX_LOG_LEVEL="$(opt log_level)"
DUPLEX_MODEL_URL="$(opt model_url)"
DUPLEX_MODEL_NAME="$(opt model_name)"
DUPLEX_MODEL_TIMEOUT="$(opt model_timeout)"
DUPLEX_MODEL_ACTIONS="$(opt model_actions)"
DUPLEX_MODEL_TOOLS_NAME="$(opt model_tools_name)"
DUPLEX_MODEL_NO_THINKING="$(opt model_no_thinking)"
DUPLEX_MODEL_MEMBER_TITLE="$(opt model_member_title)"
DUPLEX_CONNECTOR_TITLE="$(opt connector_member_title)"
DUPLEX_CONFIG_DIR=/config
DUPLEX_WAKE_TIMEOUT="$(opt wake_timeout)"
DUPLEX_REWAKE_INTERVAL="$(opt rewake_interval)"
DUPLEX_REWAKE_LIMIT="$(opt rewake_limit)"
DUPLEX_HOLD_AFTER="$(opt hold_after)"
DUPLEX_CONNECTOR_URL="$(opt connector_url)"
DUPLEX_CONNECTOR_PORT=8791
export DUPLEX_CONNECTOR_URL DUPLEX_CONNECTOR_PORT
export DUPLEX_WAKE_TIMEOUT DUPLEX_REWAKE_INTERVAL DUPLEX_REWAKE_LIMIT DUPLEX_HOLD_AFTER
export DUPLEX_MODEL_URL DUPLEX_MODEL_NAME DUPLEX_MODEL_TIMEOUT DUPLEX_MODEL_ACTIONS DUPLEX_MODEL_TOOLS_NAME
export DUPLEX_MODEL_NO_THINKING
export DUPLEX_MODEL_MEMBER_TITLE DUPLEX_CONNECTOR_TITLE DUPLEX_CONFIG_DIR DUPLEX_SERVICES DUPLEX_OWNER_NAME
export DUPLEX_DATA_DIR DUPLEX_HOST DUPLEX_PORT DUPLEX_USER_AUTH DUPLEX_PAIRING_CODE \
  DUPLEX_ENROLLMENT_TOKEN DUPLEX_WAKER_TOKEN DUPLEX_DEVICE_TOKEN_GRACE DUPLEX_LOG_LEVEL

pairing="unset"; [ -n "$DUPLEX_PAIRING_CODE" ] && pairing="set"
enrollment="unset"; [ -n "$DUPLEX_ENROLLMENT_TOKEN" ] && enrollment="set"
echo "[duplex-broker] starting, log_level=${DUPLEX_LOG_LEVEL:-info}, pairing code $pairing, enrollment token $enrollment"
exec /opt/venv/bin/python -m duplex_broker
