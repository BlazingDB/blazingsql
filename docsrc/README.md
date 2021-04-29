# BlazingSQL documentation source

## Requirements

1. Doxygen
2. Sphinx

## Building docs

There's multiple options when building the docs.

### HTML

To build the HTML docs run 

```bash
make html
```
in the `docsrc` folder.

### Doxygen

To build the documentation for the BlazingSQL engine run

```bash
make doxygen
```

### Clean build

Generating the docs for the C++ takes about 3-5 minutes each time.
To speed up this process you can run 

```bash
make clean html build_cpp=0
```

that will remove the `xml` folder from the `source`, and
will comment out the relevant parts of the `conf.py` related
to `breathe` and `exhale`.

### Publish docs

To publish the documentation in the `docs` folder run

```bash
make publish
```