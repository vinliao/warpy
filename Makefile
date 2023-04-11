.PHONY: install

install:
	python -m venv venv
	. venv/bin/activate && pip install -r requirements.txt
	@echo "Environment setup complete. Run 'source venv/bin/activate' to activate the virtual environment."
	. venv/bin/activate && python main.py download

.PHONY: codestyle
codestyle:
	. venv/bin/activate && \
	autoflake --remove-unused-variables --remove-all-unused-imports --recursive --exclude venv --in-place . && \
	isort . && \
	black .

.PHONY: lint
lint:
	. venv/bin/activate && \
	flake8 --max-line-length=79 --ignore=E203,W503 --exclude=venv . && \
	mypy . --check-untyped-defs

