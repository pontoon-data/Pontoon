IMAGE_VERSION := 0.0.1

tag-images:
	docker tag pontoon-frontend:$(IMAGE_VERSION) ghcr.io/pontoon-data/pontoon-frontend:$(IMAGE_VERSION)
	docker tag pontoon-frontend:$(IMAGE_VERSION) ghcr.io/pontoon-data/pontoon-frontend:latest

	docker tag pontoon-api:$(IMAGE_VERSION) ghcr.io/pontoon-data/pontoon-api:$(IMAGE_VERSION)
	docker tag pontoon-api:$(IMAGE_VERSION) ghcr.io/pontoon-data/pontoon-api:latest

	docker tag pontoon-worker:$(IMAGE_VERSION) ghcr.io/pontoon-data/pontoon-worker:$(IMAGE_VERSION)
	docker tag pontoon-worker:$(IMAGE_VERSION) ghcr.io/pontoon-data/pontoon-worker:latest

	docker tag pontoon-beat:$(IMAGE_VERSION) ghcr.io/pontoon-data/pontoon-beat:$(IMAGE_VERSION)
	docker tag pontoon-beat:$(IMAGE_VERSION) ghcr.io/pontoon-data/pontoon-beat:latest

	docker tag pontoon:$(IMAGE_VERSION) ghcr.io/pontoon-data/pontoon:$(IMAGE_VERSION)
	docker tag pontoon:$(IMAGE_VERSION) ghcr.io/pontoon-data/pontoon:latest

push-images:
	docker push ghcr.io/pontoon-data/pontoon-frontend:$(IMAGE_VERSION)
	docker push ghcr.io/pontoon-data/pontoon-frontend:latest

	docker push ghcr.io/pontoon-data/pontoon-api:$(IMAGE_VERSION)
	docker push ghcr.io/pontoon-data/pontoon-api:latest

	docker push ghcr.io/pontoon-data/pontoon-worker:$(IMAGE_VERSION)
	docker push ghcr.io/pontoon-data/pontoon-worker:latest

	docker push ghcr.io/pontoon-data/pontoon-beat:$(IMAGE_VERSION)
	docker push ghcr.io/pontoon-data/pontoon-beat:latest

	docker push ghcr.io/pontoon-data/pontoon:$(IMAGE_VERSION)
	docker push ghcr.io/pontoon-data/pontoon:latest

