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