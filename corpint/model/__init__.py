import logging
from sqlalchemy import create_engine
from corpint.project import Project
from corpint.common import Base
from corpint.env import DATABASE_URI, PROJECT

log = logging.getLogger(__name__)


def init_project(database_uri, name):
    """Connect to the database and create a project."""
    database_uri = database_uri or DATABASE_URI
    if database_uri is None:
        raise RuntimeError("No $DATABASE_URI is set, aborting.")
    name = name or PROJECT
    log.info("Project [%s], connected to: %s", name, database_uri)
    engine = create_engine(database_uri)
    Base.metadata.create_all(engine)
    return Project(name, engine)
