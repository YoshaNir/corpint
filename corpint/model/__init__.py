import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from corpint.model.project import Project  # noqa
from corpint.model.entity import Entity  # noqa
from corpint.model.link import Link  # noqa
from corpint.model.mapping import Mapping  # noqa
from corpint.model.common import Base

log = logging.getLogger(__name__)


def create_session(database_uri):
    """Connect to the database and create the tables."""
    if database_uri is None:
        raise RuntimeError("No $DATABASE_URI is set, aborting.")
    engine = create_engine(database_uri)
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine)
    return scoped_session(session_factory)
