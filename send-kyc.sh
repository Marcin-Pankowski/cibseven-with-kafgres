#!/usr/bin/env bash
# Sendet KYC-Anfragen auf kyc.approval.requested.
#   ./send-kyc.sh                          eine Anfrage mit Zufallsdaten
#   ./send-kyc.sh 10                       zehn Anfragen mit Zufallsdaten
#   ./send-kyc.sh KYC-4711 "Max Mustermann" eine Anfrage mit eigenen Werten
set -euo pipefail
cd "$(dirname "$0")"

names=("Max Mustermann" "Erika Musterfrau" "Hans Meier" "Anna Schmidt" "Jonas Weber" "Lea Fischer")

send() { # $1 = Business Key, $2 = Name
  # psql-Variablen + jsonb_build_object: Anführungszeichen im Namen sind so kein Problem
  docker compose exec -T kafgres psql -U postgres -q -tA -v ON_ERROR_STOP=1 \
    -v key="$1" -v name="$2" >/dev/null <<'SQL'
SELECT kafgres_produce('kyc.approval.requested', :'key',
                       jsonb_build_object('customerId', :'key', 'name', :'name')::text);
SQL
  echo "gesendet: $1 ($2)"
}

random_one() {
  send "KYC-$(( RANDOM % 90000 + 10000 ))" "${names[RANDOM % ${#names[@]}]}"
}

if [[ $# -eq 2 ]]; then
  send "$1" "$2"
elif [[ $# -eq 1 && $1 =~ ^[0-9]+$ ]]; then
  for (( i = 0; i < $1; i++ )); do random_one; done
elif [[ $# -eq 0 ]]; then
  random_one
else
  sed -n '2,5p' "$0" | sed 's/^# //'
  exit 1
fi
