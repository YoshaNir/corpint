import logging
from normality import stringify

from corpint.core import session
from corpint.model.emitter import OriginEmitter
from corpint.model.mapping import Mapping


class Project(object):
    """A particular investigation."""

    def __init__(self, name):
        self.name = stringify(name)
        self.log = logging.getLogger(self.name)

    def origin(self, name):
        return OriginEmitter(name)

    def emit_judgement(self, uida, uidb, judgement, score=None, decided=False):
        """Change the record linkage status of two entities."""
        mapping = Mapping.save(uida, uidb, judgement,
                               decided=decided, score=score)
        session.commit()
        return mapping
