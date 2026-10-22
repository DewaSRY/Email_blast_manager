# Local Postgres from the repo-root docker-compose.yaml (host port 5442).
DATABASE_URL ?= postgres://user:password@localhost:5447/mydatabase
export DATABASE_URL

.PHONY: serve
# Run the API. Pending migrations in ./migrate are applied automatically on startup.
serve:
	cargo run

.PHONY: db-up
# Start the Postgres container defined in ../../docker-compose.yaml.
db-up:
	docker compose -f ../../docker-compose.yaml up -d

.PHONY: migrate-add
# Create a new timestamped migration: make migrate-add name=add_something
# Requires sqlx-cli: cargo install sqlx-cli --no-default-features --features postgres,rustls
migrate-add:
	sqlx migrate add -r $(name) --source migrate

.PHONY: migrate-run
# Apply pending migrations manually (same ones the app runs on startup).
migrate-run:
	sqlx migrate run --source migrate

.PHONY: migrate-revert
# Roll back the most recent migration (needs a reversible `.down.sql` file).
migrate-revert:
	sqlx migrate revert --source migrate

.PHONY: migrate-info
# Show which migrations are applied and which are pending.
migrate-info:
	sqlx migrate info --source migrate
