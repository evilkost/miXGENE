#!/bin/bash

. /usr/local/bin/virtualenvwrapper.sh
workon mixgene_venv
CELERY_RDBSIG=1 celery worker --app=mixgene.celery --loglevel=DEBUG
