# make settings to make life easier
.EXPORT_ALL_VARIABLES:
.ONESHELL:
.NOTPARALLEL:

# Export UID/GID to make dev containers run with the same UID/GID as the local
# user
UID=${shell id -u}
GID=${shell id -g}
FULL_VERSION=$$(git tag --points-at HEAD)
MAJOR_VERSION=$$(echo $(FULL_VERSION) | cut -d. -f1)
DOCKER_CLI_EXPERIMENTAL=enabled

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

build_images:
	@docker buildx create --use --name=crossplat --node=crossplat && \
	docker buildx build \
		--output "type=docker,push=false" \
		--tag tonyfg/trucker:$(FULL_VERSION) \
		.

push_images:
	@docker buildx create --use --name=crossplat --node=crossplat && \
	docker buildx build \
		--platform linux/386,linux/amd64,linux/arm/v6,linux/arm/v7,linux/arm64 \
		--output "type=image,push=true" \
		--tag tonyfg/trucker:$(FULL_VERSION) \
		--tag tonyfg/trucker:$(MAJOR_VERSION) \
		.
