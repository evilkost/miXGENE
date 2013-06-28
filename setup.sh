#!/usr/bin/env sh

sudo apt-get update
sudo apt-get upgrade
 
sudo apt-get install python python-pip mysql-server mysql-common mysql-client python-mysqldb nginx
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
sudo pip install Django --upgrade
sudo pip install South --upgrade
