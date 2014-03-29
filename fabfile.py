import os

from fabric.api import *
from fabric.contrib.files import append


CONFIG = {
    "BASE_DIR": None,
    "PREFIX": None,
}


def prod():
    env.user = "kost"  # create mixgene user
    env.hosts = ["mixgene.felk.cvut.cz"]
    CONFIG["BASE_DIR"] = "/home/kost/miXGENE"
    CONFIG["PREFIX"] = "production"


def local():
    env.hosts = ["127.0.0.1"]
    CONFIG["BASE_DIR"] = os.getcwd()
    CONFIG["PREFIX"] = "development"


def debian_whezzy_backports():
    append("/etc/apt/sources.list",
           "deb http://ftp.us.debian.org/debian wheezy-backports main", use_sudo=True)

    ## For debian stable we need to add repo for R 3.0
    sudo("apt-key adv --keyserver pgp.mit.edu --recv-key 381BA480")
    append("/etc/apt/sources.list",
           "deb http://cran.r-mirror.de/bin/linux/debian wheezy-cran3/", use_sudo=True)

    ## Newer nginx
    sudo("wget 'http://nginx.org/keys/nginx_signing.key' &&  apt-key add nginx_signing.key")
    append("/etc/apt/sources.list",
           "deb http://nginx.org/packages/debian/ wheezy nginx", use_sudo=True)


def basic_apt_packages():
    packages = [
        # name, optional version
        ('python', ''),
        ('python-dev', ''),
        ('python-pip', ''),
        ('git-core', ''),
        ('tmux', ''),
        ('mysql-server', ''),
        ('mysql-common', ''),
        ('mysql-client', ''),
        ('python-mysqldb', ''),
        ('nginx', ''),
        ('redis-server', '2:2.6.13-1~bpo70+1+b1'),
        ('r-base-core', ''),
        ('libxml2-dev', ''),
        ('r-cran-xml', ''),
        ('libgraphviz-dev', ''),
        ('libatlas-dev', ''),
        ('libatlas3-base', ''),
        ('libmysqlclient-dev', ''),
        ('supervisor', ''),
    ]

    packs_def = []
    for package, version in packages:
        if version:
            packs_def.append("%s=%s" % (package, version))
        else:
            packs_def.append("%s" % (package, ))

    cmd = "apt-get install -y %s" % " ".join(packs_def)
    sudo(cmd)


def install_node_npm():
    sudo("curl --insecure https://www.npmjs.org/install.sh | bash")
    sudo("npm install bower")


def initial_install():
    debian_whezzy_backports()
    basic_apt_packages()

    update_from_gh()


def configure_supervisor():
    # with cd(CONFIG["BASE_DIR"]):
    #     sudo("cp run/supervisor/dev.conf /etc/supervisor/conf.d/mixgene.conf")

    put("run/supervisor/%s.conf" % CONFIG["PREFIX"],
        "/etc/supervisor/conf.d/mixgene.conf", use_sudo=True)

    sudo("supervisorctl reread")
    sudo("supervisorctl update")


def reload_nginx():
    sudo("/etc/init.d/nginx reload")


def stop_nginx():
    sudo("/etc/init.d/nginx stop")


def start_nginx():
    sudo("/etc/init.d/nginx start")


def start_all():
    sudo("supervisorctl start mixgene")
    sudo("supervisorctl start mixgene_notifier")
    sudo("supervisorctl start celery")


def halt_all():
    sudo("supervisorctl stop mixgene")
    sudo("supervisorctl stop mixgene_notifier")
    sudo("supervisorctl stop celery")


def run_status():
    sudo("supervisorctl status mixgene mixgene_notifier celery")


def update_from_gh():
    with settings(warn_only=True):
        run("killall gunicorn")
        run("tmux ls | awk '{print $1}' | sed 's/://g' | xargs -I{} tmux kill-session -t {}")

    # TODO: copy R

    with cd(CONFIG["BASE_DIR"]):
        run("git pull")
        with cd("mixgene_project/webapp/static"):
            run("bower install")

        run("python mixgene_project/manage.py collectstatic --noinput")
        #run("sh run_all.sh")
        # ssh and run run_all.sh manually
        sudo("pip install -r requirements.txt")
