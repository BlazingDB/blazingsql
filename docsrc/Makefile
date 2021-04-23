# Minimal makefile for Sphinx documentation
#

# Minimal makefile for Sphinx documentation
#

# You can set these variables from the command line, and also
# from the environment for the first two.
SPHINXOPTS    ?=
SPHINXBUILD   ?= sphinx-build
SOURCEDIR     = source
BUILDDIR      = build
BUILD_CPP     = $(build_cpp)

# Put it first so that "make" without argument is like "make help".
help:
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

.PHONY: help Makefile

# Catch-all target: route all unknown targets to Sphinx using the new
# "make mode" option.  $(O) is meant as a shortcut for $(SPHINXOPTS).
%: Makefile
	@export SPHINX_NO_CPP=$(BUILD_CPP); $(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

doxygen:
	@doxygen "$(SOURCEDIR)/Doxyfile"

clean:
	@rm	-rf	"$(BUILDDIR)" "$(SOURCEDIR)/xml"
	@export SPHINX_BUILD_CPP=$(BUILD_CPP); $(SPHINXBUILD) -M clean "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

publish:
	@rm -rf "../docs"
	@cp -r "$(BUILDDIR)/html" "../docs"
	@touch ../docs/.nojekyll