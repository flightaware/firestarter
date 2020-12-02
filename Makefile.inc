BLACK=black
PYLINT=pylint
PYTHON=python3

venv: venv/bin/activate

venv/bin/activate:
	test -d venv || $(PYTHON) -m venv venv
	. venv/bin/activate && $(PYTHON) -m pip install pip-tools

pip-compile: venv
	. venv/bin/activate && make -C requirements all

pip-sync: venv pip-compile
	. venv/bin/activate && venv/bin/pip-sync $(wildcard requirements/*.txt)

pip-sync-travis:
	$(PYTHON) -m pip install pip-tools
	pip-sync $(wildcard requirements/*.txt)

docker-setup: venv pip-sync

type-check:
	. venv/bin/activate && mypy --check-untyped-defs .

format:
	. venv/bin/activate && $(PYTHON) -m $(BLACK) --line-length=100 .

lint:
	. venv/bin/activate && $(PYTHON) -m $(PYLINT) *.py

check: lint type-check

test: 
	. venv/bin/activate && $(PYTHON) -m unittest discover -s test

test-travis: 
	$(PYTHON) -m unittest discover -s test

run:
	docker-compose -f ../docker-compose.yml up --build $(PROGNAME)

.PHONY: venv pip-compile pip-sync docker-setup type-check format lint check test run
