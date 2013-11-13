#!/usr/bin/env sh
#TODO: move pip install to requirements.txt

sudo apt-get update
sudo apt-get upgrade

sudo apt-get install python python-pip mysql-server mysql-common mysql-client python-mysqldb nginx redis-server gunicorn  r-base-core libxml2-dev r-cran-xml
# during installation you will be asked for a password for the root
# underworldHj53

mysql -u root -p
# enter root's password
# SQL begin
CREATE DATABASE django_miGENEX;
CREATE USER 'mixgene_app'@'localhost' IDENTIFIED BY 'qeaOHN73aBs2';
GRANT ALL PRIVILEGES on django_miGENEX.* TO 'mixgene_app'@'localhost' WITH GRANT OPTION;
FLUSH PRIVILEGES;
quit
# SQL end

# now it should be possible to auth:
#   "mysql django_miGENEX -u migenex_app -p" with  password qeaOHN73aBs2


# installing Django from PIP
sudo pip install -U Django South Celery django-celery redis hiredis pandas biopython rpy2 fysom flower


# init base db tables and prepare for migrations
#cd miXGENE/mixgene_project
python manage.py syncdb

#do db migrations
python manage.py migrate

mkdir -p /home/kost/res/mixgene_workdir/media/data/cache/
