
export ENV=test
export POSTGRES_HOST=localhost
export POSTGRES_PORT=5436
export POSTGRES_USER=test
export POSTGRES_PASSWORD=test
export POSTGRES_DATABASE=pontoon
export ALLOW_ORIGIN=http://localhost:3000
export JWT_ALGORITHM=HS256
export JWT_SIGNING_KEY=test_key
export SKIP_TRANSFERS=true

dev:
	pip install requirements.txt
	pip install ../data-transfer/pontoon/
	pip install -e .
		
test:
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
	cd app && pytest
	@echo "==> Cleaning up test database..."
	docker stop api_test_db || true
	docker rm api_test_db || true
	@echo "==> Done."
