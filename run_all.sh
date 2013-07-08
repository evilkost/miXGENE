### mysql
# system


### redis
# system

### celery
python mixgene_project/manage.py celery worker --loglevel=inf &

### gunicorn
gunicorn -b 127.0.0.1:9431 mixgene.wsgi:application &

### nginx
# system


