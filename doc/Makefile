# Minimal makefile for Sphinx documentation
#

# You can set these variables from the command line, and also
# from the environment for the first two.
SPHINXOPTS    ?=
SPHINXBUILD   ?= sphinx-build
SOURCEDIR     = .
BUILDDIR      = _build

REPO_RAW      = https://raw.githubusercontent.com/sentinel-energy/friendly_data_example/master
DATA_CSV      = $(REPO_RAW)/data/flow_in_sum.csv
TECH_CSV      = $(REPO_RAW)/technology.csv

# Put it first so that "make" without argument is like "make help".
help:
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

.PHONY: help Makefile

# Catch-all target: route all unknown targets to Sphinx using the new
# "make mode" option.  $(O) is meant as a shortcut for $(SPHINXOPTS).
%: Makefile
	@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

# disable catch-all for dependency in the rule below
ecosystem.txt:
	:

_static/images/ecosystem.png: ecosystem.txt
	wget --no-clobber https://code.orgmode.org/bzg/org-mode/raw/master/contrib/scripts/ditaa.jar
	java -Dfile.encoding=UTF-8 -jar ditaa.jar $< $@

tutorial/flow_in_sum.csv:
	wget -q $(DATA_CSV) -O - | ./trim-csv.awk > $@

tutorial/technology.csv:
	wget -q $(TECH_CSV) -O - | sed -ne 1p -e '/electric_heater\|light_transport_ev/p' > $@
