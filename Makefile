.PHONY: install
install:
	python -m venv venv
	. venv/bin/activate && pip install -r requirements.txt

.PHONY: format
format:
	source venv/bin/activate && \
	autoflake --remove-unused-variables --remove-all-unused-imports --recursive --exclude venv --in-place . && \
	isort . && \
	black .

.PHONY: lint
lint:
	source venv/bin/activate && \
	ruff check . && \
	vulture . --min-confidence 80 --exclude venv
	mypy . --check-untyped-defs

.PHONY: dev
dev:
	source venv/bin/activate && \
	uvicorn src.app.api:app --reload