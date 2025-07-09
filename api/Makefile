db-migrate-dev:
	python -m pgmigrate -c postgresql://dev:dev@localhost:5432/pontoon -d db/ -t 1 migrate


# Production database migration
db-migrate:
	python -m pgmigrate -d db/ -t 1 migrate 

app-test:
	@echo "==> Stopping test database (if running)..."
	docker stop api_test_db || true
	docker rm api_test_db || true
	@echo "==> Starting new test database..."
	docker run --name api_test_db -e POSTGRES_USER=test -e POSTGRES_PASSWORD=test -e POSTGRES_DB=pontoon -p 5436:5432 -d postgres:16
	@echo "==> Waiting for database to start..."
	@sleep 10
	@echo "==> Running schema migrations..."
	python -m pgmigrate -c postgresql://test:test@localhost:5436/pontoon -d db/ -t 1 migrate
	@echo "==> Running unit tests..."
	cd app && ENV=test pytest
	@echo "==> Cleaning up test database..."
	docker stop api_test_db || true
	docker rm api_test_db || true
	@echo "==> Done."

app-install-dev:
	pip install -e .

