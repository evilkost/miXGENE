#!/bin/bash
SESSION=$USER

### mysql
# system

### nginx
# system

### redis
# system

### celery
#python mixgene_project/manage.py celery worker --loglevel=DEBUG &

### gunicorn
#gunicorn -b 127.0.0.1:9431 mixgene.wsgi:application &

tmux -2 new-session -d -s $SESSION

# Setup a window for tailing log files
tmux new-window -t $SESSION:1 -n 'WWW-mixgene'
tmux send-keys "" C-m


# Setup a Celery window
tmux split-window -h
tmux select-pane -t 0
#tmux send-keys "python mixgene_project/manage.py runserver 127.0.0.1:9431" C-m
tmux send-keys "cd mixgene_project/ && gunicorn -b 127.0.0.1:9431 mixgene.wsgi:application" C-m
tmux split-window -v
tmux select-pane -t 1
tmux send-keys "cd notify_server/ && node server.js" C-m
tmux resize-pane -R 20
tmux select-pane -t 2
tmux send-keys "python mixgene_project/manage.py celery worker --loglevel=INFO" C-m
tmux split-window -v
tmux select-pane -t 3
tmux send-keys "python mixgene_project/manage.py celery flower --port=5555" C-m

tmux select-pane -t 0

# Set default window
#tmux select-window -t $SESSION:1
# Attach to session
tmux -2 attach-session -t $SESSION
