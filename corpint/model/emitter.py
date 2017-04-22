import logging
from hashlib import sha1
from normality import stringify

from corprint.model.mapping import Mapping
from corprint.model.entity import Entity


class Emitter(object):
    """Emitters are used to generate entities within the database."""

    def __init__(self, project, origin, query_uid=None, match_uid=None):
        self.project = project
        self.origin = stringify(origin)
        if self.origin is None:
            raise ValueError("Invalid origin")

        self.log = logging.getLogger('%s.%s' % (project.name, self.origin))
        self.query_uid = query_uid
        self.match_uid = match_uid
        self.disabled = False

        if query_uid and match_uid:
            judgement = Mapping.get_judgement(project, query_uid, match_uid)
            self.disabled = judgement is False

    def uid(self, *args):
        """Generate a unique identifier for an entity."""
        uid = sha1(self.origin.encode('utf-8'))
        has_args = False
        for arg in args:
            arg = stringify(arg)
            if arg is None:
                return None
            has_args = True
            uid.update(arg.encode('utf-8'))
        if not has_args:
            raise ValueError("No unique key given!")
        return uid.hexdigest()

    def emit_entity(self, data):
        """Create or update an entity in the context of this emitter."""
        if self.disabled:
            return
        entity = Entity.save(self.project, data, self.origin,
                             query_uid=self.query_uid,
                             match_uid=self.match_uid)
        self.project.session.commit()
        return entity

    def emit_judgement(self, uida, uidb, judgement, score=None, decided=False):
        """Change the record linkage status of two entities."""
        if self.disabled:
            return
        mapping = Mapping.save(self.project, uida, uidb, judgement,
                               decided=decided, score=score)
        self.project.session.commit()
        return mapping

    def clear(self):
        Entity.delete_by_origin(self.project, self.origin,
                                query_uid=self.query_uid,
                                match_uid=self.match_uid)
        self.project.session.commit()


class OriginEmitter(Emitter):
    """Generate entities without a result context."""

    def __init__(self, project, name):
        super(OriginEmitter, self).__init__(project, name)

    def result(self, query_uid, match_uid):
        """Create an emitter for a specific result."""
        return ResultEmitter(self, query_uid, match_uid)

    def __repr__(self):
        return '<OriginEmitter(%r, %r)>' % (self.project.name, self.origin)


class ResultEmitter(Emitter):
    """Generate entities inside a result context."""

    def __init__(self, origin, query_uid, match_uid):
        super(OriginEmitter, self).__init__(origin.project,
                                            origin.origin,
                                            query_uid=query_uid,
                                            match_uid=match_uid)

    def emit_entity(self, data):
        # Enrichment results are first held as inactive and become active only
        # once the judgement between the query and result entities is confirmed
        data['active'] = False
        return super(ResultEmitter, self).emit_entity(data)
        # TODO: generate score / mapping candidate

    def __repr__(self):
        return '<ResultEmitter(%r, %r, %r, %r)>' % (self.project.name,
                                                    self.origin,
                                                    self.query_uid,
                                                    self.match_uid)
