install:
	pip install -r requirements.txt

install-dev:
	# Installing requirements in a single command to ensure constraints from both requirement files are resolved together
	pip install -r requirements.txt -r requirements-dev.txt

test-static:
	ruff check dags/

test-unit:
	pytest tests/unit/

test: test-static test-unit

format:
	ruff format dags/ tests
	ruff check --fix dags/ tests/
