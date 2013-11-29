#!/usr/bin/env sh

export DJANGO_SETTINGS_MODULE=mixgene.settings

nosetests test/structures.py
