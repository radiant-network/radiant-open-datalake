install:
	pip install -r requirements.txt -c constraints-python3.12.txt

install-dev:
	# Installing requirements in a single command to ensure constraints from both requirement files are resolved together
	pip install -r requirements.txt -r requirements-dev.txt -c constraints-python3.12.txt

	# To be able to run some unit tests
	airflow db reset -y

test-static:
	ruff check dags/

test-unit:
	pytest tests/unit/

test: test-static test-unit

format:
	ruff format dags/ tests
	ruff check --fix dags/ tests/


