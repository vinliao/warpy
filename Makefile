.PHONY: install

install:
	python -m venv venv
	. venv/bin/activate && pip install -r requirements.txt
	@echo "Environment setup complete. Run 'source venv/bin/activate' to activate the virtual environment."
	python main.py download
	python main.py init
