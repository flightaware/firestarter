objects = $(wildcard *.in)
outputs := $(objects:.in=.txt)

.PHONY: all
all: $(outputs)

%.txt: %.in
	. ../venv/bin/activate && pip-compile --verbose --build-isolation --generate-hashes --output-file $@ $<

dev.txt: base.txt
