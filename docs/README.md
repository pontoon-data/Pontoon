# Docs Website

This is the public docs website for Pontoon, powered by [Material for MkDocs](https://github.com/squidfunk/mkdocs-material).

To get started, create a python virtual environment and install Material for MkDocs

```sh
pip install mkdocs-material
```

Run the docs website. Recommended to run it on a different port than the default (8000) since FastAPI uses the same default port.

```sh
mkdocs serve --dev-addr localhost:9000
```
