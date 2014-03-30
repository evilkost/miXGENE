import os

from fabric.api import *
from fabric.contrib.files import append


CONFIG = {
    "ROOT_DIR": None,
    "RES_DIR": None,
    "PREFIX": None,
}


def prod():
    env.user = "kost"  # create mixgene user
    env.hosts = ["mixgene.felk.cvut.cz"]
    CONFIG["ROOT_DIR"] = "/home/kost"
    CONFIG["RES_DIR"] = "/home/kost/res"
    CONFIG["PREFIX"] = "production"


def local():
    env.hosts = ["127.0.0.1"]
    CONFIG["ROOT_DIR"] = os.path.abspath(os.path.join(os.getcwd(), os.pardir))
    CONFIG["RES_DIR"] = "/home/kost/res/mixgene_mixgene_workdir"
    CONFIG["PREFIX"] = "development"
    print CONFIG


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


def basic_packages():
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
        ('pkg-config', ''),
        ('graphviz', ''),
        ('libgraphviz-dev', ''),
    ]

    packs_def = []
    for package, version in packages:
        if version:
            packs_def.append("%s=%s" % (package, version))
        else:
            packs_def.append("%s" % (package, ))

    cmd = "apt-get install -y %s" % " ".join(packs_def)
    sudo(cmd)
    sudo("pip install virtualenvwrapper")


def install_node_npm():
    sudo("curl --insecure https://www.npmjs.org/install.sh | bash")
    sudo("npm install bower")


def install_r_packages():
    raise NotImplementedError("Install R packages should be here")


def configure_supervisor():
    put("run/supervisor/%s.conf" % CONFIG["PREFIX"],
        "/etc/supervisor/conf.d/mixgene.conf", use_sudo=True)

    sudo("supervisorctl reread")
    sudo("supervisorctl update")


def put_nginx_conf():
    put("nginx/mixgene.%s" % CONFIG["PREFIX"],
        "/etc/nginx/conf.d/mixgene.conf", use_sudo=True)

    sudo("/etc/init.d/nginx reload")


def init_venv():
    with prefix(". /usr/local/bin/virtualenvwrapper.sh"):
        run("mkvirtualenv mixgene_venv")


def git_clone():
    with cd(CONFIG["ROOT_DIR"]):
        run("git clone https://github.com/evilkost/miXGENE.git")


def mk_dirs():
    with cd(CONFIG["RES_DIR"]):
        for dir_spec in [
            "logs",
            "media",
            "media/data",
            "media/data/R",
            "media/data/cache",
            "media/data/broad_institute",
        ]:
            try:
                os.makedirs(dir_spec)
            except OSError:
                pass


def do_updates():
    with cd(CONFIG["ROOT_DIR"] + "/miXGENE"):
        with prefix(". /usr/local/bin/virtualenvwrapper.sh; workon mixgene_venv"):
            run("pip install -r requirements.txt")

            with cd("mixgene_project/webapp/static"):
                run("bower install")

            with cd("mixgene_project/"):
                run("python manage.py collectstatic --noinput")
                run("python manage.py syncdb")
                run("python manage.py migrate")

            with cd("notify_server"):
                run("npm install")

        sudo("chmod +x run/*")


def update_from_gh():
    # TODO: copy R
    halt_all()
    with cd(CONFIG["ROOT_DIR"] + "/miXGENE"):
        run("git pull")

    do_updates()

    start_all()

### Control methods

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

def restart_all():
    halt_all()
    start_all()

def run_status():
    sudo("supervisorctl status mixgene mixgene_notifier celery")


###

def initial_install():
    debian_whezzy_backports()
    basic_packages()
    install_node_npm()
    install_r_packages()

    configure_supervisor()
    put_nginx_conf()

    git_clone()
    mk_dirs()
    update_from_gh()

    start_all()