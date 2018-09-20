#!/usr/bin/env bash
python setup.py sdist register bdist_egg upload
rm -rf dist
