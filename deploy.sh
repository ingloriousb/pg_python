#!/usr/bin/env bash
python setup.py sdist register bdist_egg
python3.11 -m twine upload dist/pg_python-1.602.tar.gz
rm -rf dist
