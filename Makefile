PG      := $(HOME)/opt/pg
PGDATA  := $(HOME)/opt/pgdata
PY      := .venv/bin/python
export LD_LIBRARY_PATH := $(PG)/lib:$(LD_LIBRARY_PATH)

.PHONY: install db stop run test harness load clean

install:
	python3 -m venv .venv
	$(PY) -m pip install -q --upgrade pip
	$(PY) -m pip install -q fastapi uvicorn asyncpg httpx

db:
	@test -d $(PGDATA) || $(PG)/bin/initdb -D $(PGDATA) -U $$USER --auth=trust -E UTF8
	@grep -q "port = 5433" $(PGDATA)/postgresql.conf || \
	  printf "max_connections = 200\nlisten_addresses = '127.0.0.1'\nport = 5433\n" >> $(PGDATA)/postgresql.conf
	@$(PG)/bin/pg_ctl -D $(PGDATA) status >/dev/null 2>&1 || \
	  $(PG)/bin/pg_ctl -D $(PGDATA) -l $(HOME)/opt/pg.log -w start
	@$(PG)/bin/createdb -h 127.0.0.1 -p 5433 -U $$USER ledger 2>/dev/null || true
	@echo "postgres ready on 127.0.0.1:5433"

stop:
	-@pkill -f "uvicorn app.main:app"
	-@$(PG)/bin/pg_ctl -D $(PGDATA) -w stop

run: db
	$(PY) -m uvicorn app.main:app --host 127.0.0.1 --port 8000

test: db
	$(PY) -m tests.test_ledger

harness:
	$(PY) -m harness.concurrency -n 500 --dup-keys 100 --copies 5 --amount 100

load:
	$(PY) -m harness.loadtest --path /transfer -c 64 -n 60 --dup-ratio 0.3 --accounts 64
	$(PY) -m harness.loadtest --path /naive/transfer -c 64 -n 60 --dup-ratio 0.3 --accounts 64

clean:
	rm -f results_*.json r[0-9].json rr.json server.log
	find . -name __pycache__ -type d -exec rm -rf {} +
