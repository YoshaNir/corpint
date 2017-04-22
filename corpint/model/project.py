import logging
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker
from corpint.model.emitter import OriginEmitter


class Project(object):
    """A particular investigation."""

    def __init__(self, name, engine):
        self.name = name
        self.session_factory = sessionmaker(bind=engine)
        self.session = scoped_session(self.session_factory)
        self.log = logging.getLogger(self.name)

    def origin(self, name):
        return OriginEmitter(self, name)
