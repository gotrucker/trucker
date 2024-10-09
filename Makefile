# make settings to make life easier
.EXPORT_ALL_VARIABLES:
.ONESHELL:
.NOTPARALLEL:

# Export UID/GID to make dev containers run with the same UID/GID as the local
# user
UID=${shell id -u}
GID=${shell id -g}

#####################
# Development tools #
#####################
dev:
	touch .env
	docker compose up -d --remove-orphans -t 1

sh:
	docker compose exec go ash

clean:
	docker compose down --remove-orphans -v -t 1
	docker compose rm -f -s -v
	docker image rm -f trucker-go trucker-pg_input trucker-pg_input_replica trucker-pg_output pg-with-wal2json
