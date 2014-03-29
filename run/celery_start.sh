#!/bin/bash

. /usr/local/bin/virtualenvwrapper.sh
workon mixgene_venv
celery worker --app=mixgene.celery --loglevel=DEBUG
