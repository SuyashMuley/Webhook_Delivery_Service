#!/usr/bin/env bash
# Verifies the services can talk to Postgres over TLS with certificate verification:
# a throwaway Postgres with a private CA, then every sslmode the services might be given.
# Needs Docker, openssl and Go. Run from anywhere: ./deploy/tls/check.sh
set -uo pipefail
cd "$(dirname "$0")/../.."
D=$(mktemp -d); trap 'docker rm -f pg-tls >/dev/null 2>&1; rm -rf "$D"' EXIT
fail=0

openssl req -x509 -newkey rsa:2048 -nodes -keyout "$D/ca.key" -out "$D/ca.crt" -days 2 -subj "/CN=webhook-test-ca" 2>/dev/null
openssl req -newkey rsa:2048 -nodes -keyout "$D/server.key" -out "$D/server.csr" -subj "/CN=db.test" 2>/dev/null
echo "subjectAltName=DNS:localhost,DNS:db.test" > "$D/san.ext"
openssl x509 -req -in "$D/server.csr" -CA "$D/ca.crt" -CAkey "$D/ca.key" -CAcreateserial -out "$D/server.crt" -days 2 -extfile "$D/san.ext" 2>/dev/null

docker rm -f pg-tls >/dev/null 2>&1
docker run -d --name pg-tls -p 15433:5432 -e POSTGRES_USER=webhooks -e POSTGRES_PASSWORD=webhooks -e POSTGRES_DB=webhooks \
  -v "$D:/certs:ro" postgres:17 sh -c 'cp /certs/server.crt /certs/server.key /var/lib/postgresql/ && chown postgres:postgres /var/lib/postgresql/server.* && chmod 600 /var/lib/postgresql/server.key && exec docker-entrypoint.sh postgres -c ssl=on -c ssl_cert_file=/var/lib/postgresql/server.crt -c ssl_key_file=/var/lib/postgresql/server.key' >/dev/null
for _ in $(seq 1 40); do docker exec pg-tls pg_isready -U webhooks -q 2>/dev/null && break; sleep 1; done
B="postgres://webhooks:webhooks@localhost:15433/webhooks"
DATABASE_URL="$B?sslmode=verify-full&sslrootcert=$D/ca.crt" go run ./cmd/migrate up >/dev/null 2>&1 || { echo "migration over TLS failed"; exit 1; }

check() { # description expected(CONNECTED|REFUSED) url
  local out; out=$(DATABASE_URL="$3" go run ./cmd/admin create-tenant "tls-$RANDOM" 2>&1 | tail -1)
  local got=REFUSED; case "$out" in *-*-*-*-*) got=CONNECTED;; esac
  local mark=PASS; [ "$got" != "$2" ] && { mark=FAIL; fail=1; }
  printf "  %-4s %-60s expected %-9s got %s\n" "$mark" "$1" "$2" "$got"
}
echo "connection modes:"
check "verify-full + private CA + matching host name"            CONNECTED "$B?sslmode=verify-full&sslrootcert=$D/ca.crt"
check "verify-full without the CA (system trust only)"           REFUSED   "$B?sslmode=verify-full"
check "verify-full, CA given, host name not in the certificate"  REFUSED   "postgres://webhooks:webhooks@127.0.0.1:15433/webhooks?sslmode=verify-full&sslrootcert=$D/ca.crt"
check "require (encrypted but NOT verified; do not use in prod)" CONNECTED "$B?sslmode=require"

echo "the real API process:"
make bin >/dev/null 2>&1
DATABASE_URL="$B?sslmode=verify-full&sslrootcert=$D/ca.crt" HTTP_ADDR=127.0.0.1:18100 METRICS_ADDR=127.0.0.1:19100 LOG_LEVEL=error ./bin/api >/dev/null 2>&1 & API=$!
sleep 6
code=$(curl -s -o /dev/null -w '%{http_code}' localhost:18100/readyz)
sess=$(docker exec pg-tls psql -U webhooks -d webhooks -tAc "select count(*) from pg_stat_ssl s join pg_stat_activity a using(pid) where s.ssl and a.usename='webhooks' and a.backend_type='client backend' and a.pid<>pg_backend_pid()" 2>/dev/null)
kill $API 2>/dev/null; wait $API 2>/dev/null
[ "$code" = "200" ] && [ "${sess:-0}" -ge 1 ] && echo "  PASS  API ready (HTTP $code) over $sess verified, encrypted session(s)" || { echo "  FAIL  API readyz=$code encrypted sessions=${sess:-0}"; fail=1; }
echo; [ $fail = 0 ] && echo "ALL TLS CHECKS PASSED" || { echo "SOME TLS CHECKS FAILED"; exit 1; }
