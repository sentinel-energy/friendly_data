# Minimal makefile for Sphinx documentation
#

# You can set these variables from the command line, and also
# from the environment for the first two.
SPHINXOPTS    ?=
SPHINXBUILD   ?= sphinx-build
SOURCEDIR     = .
BUILDDIR      = _build

REPO_RAW      = https://raw.githubusercontent.com/sentinel-energy/friendly_data_example/master
DATA_DIR      = _static/data
DATA_CSV      = $(DATA_DIR)/flow_out_sum.csv $(DATA_DIR)/iamc.csv
TECH_CSV      = $(DATA_DIR)/technology.csv

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

$(DATA_CSV):
	wget -q $(subst $(DATA_DIR)/,$(REPO_RAW)/,$@) -O - | ./trim-csv.awk > $@

$(TECH_CSV):
	wget -q $(subst $(DATA_DIR)/,$(REPO_RAW)/,$@) -O - | sed -ne 1p -e '/wind_onshore\|wind_offshore\|nuclear/p' > $@
