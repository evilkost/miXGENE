# Setup instruction, not complete script
#TODO: move pip install to requirements.txt

sudo apt-get update
#sudo apt-get upgrade

sudo apt-get install python python-pip git-core tmux mysql-server mysql-common \
    mysql-client python-mysqldb nginx redis-server gunicorn  \
    r-base-core libxml2-dev r-cran-xml \
    python-nose python-django-nose

# during installation you will be asked for a password for the root $DB_ROOT_PASSWORD
mysql -u root -p
# enter root's password
# SQL begin
CREATE DATABASE django_miGENEX;
CREATE USER 'mixgene_app'@'localhost' IDENTIFIED BY '$DATABASE_PASSWORD';
GRANT ALL PRIVILEGES on django_miGENEX.* TO 'mixgene_app'@'localhost' WITH GRANT OPTION;
FLUSH PRIVILEGES;
quit
# SQL end

# now it should be possible to auth:
#   "mysql django_miGENEX -u migenex_app -p" with  password qeaOHN73aBs2


# installing Django from PIP
sudo pip install -U Django South Celery django-celery redis hiredis pandas biopython rpy2 fysom flower django-extensions

# init base db tables and prepare for migrations
# cd to appropriate folder
git clone https://github.com/evilkost/miXGENE.git

# creating local_settings.py configuration
cd miXGENE
cp mixgene_project/mixgene/local_settings.py{.example,}
# and fill it with private values


# create basic db tables
python mixgene_project/manage.py syncdb

#do db migrations for tables under south control
python mixgene_project/manage.py migrate

#put css, js, images to location defined in local_settings
python mixgene_project/manage.py collectstatic --noinput

mkdir -p $BASE_DIR/media/data/cache/
mkdir -p $BASE_DIR/media/data/broad_institute/

# run server
sh run_all.sh

