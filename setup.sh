#!/bin/sh
# Setup instruction, not complete script
#TODO: move pip install to requirements.txt

### For debian stable we need to add repo for R 3.0
#apt-key adv --keyserver pgp.mit.edu --recv-key 381BA480
#echo "deb http://cran.r-mirror.de/bin/linux/debian wheezy-cran3/" >> /etc/apt/sources.list

### Newer nginx
#wget http://nginx.org/keys/nginx_signing.key &&  apt-key add nginx_signing.key
#echo "deb http://nginx.org/packages/debian/ wheezy nginx" >> /etc/apt/sources.list
#
#echo "deb http://ftp.us.debian.org/debian wheezy-backports main" >> /etc/apt/sources.list
#
#
#sudo apt-get update
##sudo apt-get upgrade
#
#sudo apt-get install -y python python-dev python-pip git-core tmux mysql-server mysql-common \
#    mysql-client python-mysqldb nginx redis-server=2:2.6.13-1~bpo70+1+b1 gunicorn  \
#    r-base-core libxml2-dev r-cran-xml libgraphviz-dev \
#    python-nose python-django-nose libatlas-dev libatlas3-base nodejs-legacy
#
#
#curl --insecure https://www.npmjs.org/install.sh | bash
#npm install bower
#
#
## installing Django from PIP
#sudo pip install -U Django South Celery django-celery redis hiredis \
#    pandas biopython rpy2 fysom flower django-extensions \
#    numpy=1.6.2 scikit-learn markdown pygraphviz python-redis-lock



# during installation you will be asked for a password for the root $DB_ROOT_PASSWORD
mysql -u root -p
# enter root's password
# SQL begin
CREATE DATABASE django_miXGENE;
#CREATE USER 'mixgene_app'@'localhost' IDENTIFIED BY '$DATABASE_PASSWORD';
CREATE USER 'mixgene_app'@'localhost' IDENTIFIED BY 'qeaOHN73aBs2';
GRANT ALL PRIVILEGES on django_miXGENE.* TO 'mixgene_app'@'localhost' WITH GRANT OPTION;
FLUSH PRIVILEGES;
quit
# SQL end

# now it should be possible to auth:
#   "mysql django_miGENEX -u migenex_app -p qeaOHN73aBs2"


# init base db tables and prepare for migrations
# cd to appropriate folder
#git clone https://github.com/evilkost/miXGENE.git

# creating local_settings.py configuration
cd miXGENE
cp mixgene_project/mixgene/local_settings.py{.example,}
cd ..
# and fill it with private values


# create basic db tables
#python mixgene_project/manage.py syncdb

#do db migrations for tables under south control
#python mixgene_project/manage.py migrate

# install common JS/CSS files
#cd mixgene_project/webapp/static && bower install && cd ../../..


#put css, js, images to location defined in local_settings
#python mixgene_project/manage.py collectstatic --noinput

# download node modules
#cd notify_server/
#node install
#cd ..

sudo ln -s `pwd`/nginx/mixgene.production /etc/nginx/conf.d/mixgene.conf
sudo /etc/init.d/nginx restart

mkdir -p $BASE_DIR/logs/
mkdir -p $BASE_DIR/media/data/cache/
mkdir -p $BASE_DIR/media/data/broad_institute/

mkdir -p $BASE_DIR/media/data/R/
# instal R modules
cp R/ranking.Methods.r $BASE_DIR/media/data/R/

# install.packages("e1071")
# source("http://bioconductor.org/biocLite.R")
# biocLite()
# biocLite("limma")


# run server
sh run_all.sh

