import logging
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning

from corpint.model import init_project
from corpint.schema import TYPES, COMPANY, ORGANIZATION, PERSON, OTHER  # noqa

log = logging.getLogger(__name__)


def project(name=None, database_uri=None):
    requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('alembic').setLevel(logging.WARNING)
    logging.getLogger('zeep').setLevel(logging.WARNING)
    logging.getLogger('httpstream').setLevel(logging.WARNING)
    logging.getLogger('neo4j').setLevel(logging.WARNING)

    init_project(name, database_uri=database_uri)
