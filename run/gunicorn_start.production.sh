#!/bin/bash

NAME="mixgene_app"                                # Name of the application

# TODO: create `mixgene` user
# Django project directory
DJANGODIR=/home/kost/miXGENE/mixgene_project
#SOCKFILE=$DJANGODIR/run/gunicorn.sock  # we will communicte using this unix socket
USER=kost                                         # the user to run as
GROUP=kost                                        # the group to run as
NUM_WORKERS=3                                     # how many worker processes should Gunicorn spawn
DJANGO_SETTINGS_MODULE=mixgene.settings             # which settings file should Django use
DJANGO_WSGI_MODULE=mixgene.wsgi                     # WSGI module name

echo "Starting $NAME as `whoami`"

# Activate the virtual environment
cd $DJANGODIR
#source ../bin/activate
workon mixgene_venv
export DJANGO_SETTINGS_MODULE=$DJANGO_SETTINGS_MODULE
export PYTHONPATH=$DJANGODIR:$PYTHONPATH

# Create the run directory if it doesn't exist
RUNDIR=$(dirname $SOCKFILE)
test -d $RUNDIR || mkdir -p $RUNDIR

# Start your Django Unicorn
# Programs meant to be run under supervisor should not daemonize themselves (do not use --daemon)
exec gunicorn ${DJANGO_WSGI_MODULE}:application \
  --name $NAME \
  --workers $NUM_WORKERS \
  --user=$USER --group=$GROUP \
  --log-level=debug \
  --bind=127.0.0.1:9431
  #--bind=unix:$SOCKFILE

deactivate