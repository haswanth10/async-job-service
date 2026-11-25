#!/usr/bin/env bash
# Starts a locally installed Postgres and creates the "jobs" database the app and the tests expect:
#   jdbc:postgresql://localhost:5432/jobs   user: postgres   password: postgres
#
# Install Postgres first if needed:  sudo apt-get update && sudo apt-get install -y postgresql
set -euo pipefail

sudo service postgresql start

for _ in $(seq 1 30); do
  if sudo runuser -u postgres -- pg_isready -q; then break; fi
  sleep 1
done

sudo runuser -u postgres -- psql -v ON_ERROR_STOP=1 -c "ALTER USER postgres PASSWORD 'postgres';"
if ! sudo runuser -u postgres -- psql -tAc "SELECT 1 FROM pg_database WHERE datname = 'jobs'" | grep -q 1; then
  sudo runuser -u postgres -- createdb jobs
fi

echo "Postgres is ready: jdbc:postgresql://localhost:5432/jobs (postgres/postgres)"
