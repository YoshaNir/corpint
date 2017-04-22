import logging
import requests
from os import environ
from werkzeug.local import LocalProxy
from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
logging.basicConfig(level=logging.INFO)
logging.getLogger('requests').setLevel(logging.WARNING)
logging.getLogger('alembic').setLevel(logging.WARNING)
logging.getLogger('zeep').setLevel(logging.WARNING)
logging.getLogger('httpstream').setLevel(logging.WARNING)
logging.getLogger('neo4j').setLevel(logging.WARNING)


class Config():
    project_name = environ.get('CORPINT_PROJECT', 'default')
    database_uri = environ.get('DATABASE_URI')
    neo4j_uri = environ.get('NEO4J_URI')


config = Config()


def get_project():
    if not hasattr(config, 'project'):
        from corpint.model import Project
        config.project = Project(config.project_name)
    return config.project


def get_session():
    if not hasattr(config, 'session'):
        from corpint.model import create_session
        config.session = create_session(config.database_uri)
    return config.session


project = LocalProxy(get_project)
session = LocalProxy(get_session)
