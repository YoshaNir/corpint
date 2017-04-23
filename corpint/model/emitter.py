import logging
from hashlib import sha1
from normality import stringify

from corpint.core import session, project
from corpint.model.mapping import Mapping
from corpint.model.entity import Entity
from corpint.model.link import Link


class Emitter(object):
    """Emitters are used to generate entities within the database."""

    def __init__(self, origin, query_uid=None, match_uid=None):
        self.origin = stringify(origin)
        if self.origin is None:
            raise ValueError("Invalid origin")

        self.log = logging.getLogger('%s.%s' % (project.name, self.origin))
        self.query_uid = query_uid
        self.match_uid = match_uid
        self.judgement = False
        self.disabled = False

        if query_uid and match_uid:
            self.judgement = Mapping.get_judgement(query_uid, match_uid)
            self.disabled = self.judgement is False

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
        return unicode(uid.hexdigest())

    def emit_entity(self, data):
        """Create or update an entity in the context of this emitter."""
        if self.disabled:
            return
        entity = Entity.save(dict(data), self.origin,
                             query_uid=self.query_uid,
                             match_uid=self.match_uid)
        session.commit()
        return entity

    def emit_link(self, data):
        """Create or update a link in the context of this emitter."""
        if self.disabled:
            return
        entity = Link.save(dict(data), self.origin)
        session.commit()
        return entity

    def emit_judgement(self, uida, uidb, judgement, score=None, decided=False):
        """Change the record linkage status of two entities."""
        if self.disabled:
            return
        return project.emit_judgement(uida, uidb, judgement, decided=decided,
                                      score=score)

    def entity_exists(self, uid):
        """Check if the given entity exists in this context."""
        entity = Entity.get(uid, query_uid=self.query_uid,
                            match_uid=self.match_uid)
        return entity is not None

    def clear(self):
        Entity.delete_by_origin(self.origin,
                                query_uid=self.query_uid,
                                match_uid=self.match_uid)
        session.commit()


class OriginEmitter(Emitter):
    """Generate entities without a result context."""

    def __init__(self, name):
        super(OriginEmitter, self).__init__(name)

    def result(self, query_uid, match_uid):
        """Create an emitter for a specific result."""
        return ResultEmitter(self, query_uid, match_uid)

    def __repr__(self):
        return '<OriginEmitter(%r)>' % (self.origin)


class ResultEmitter(Emitter):
    """Generate entities inside a result context."""

    def __init__(self, origin, query_uid, match_uid):
        super(ResultEmitter, self).__init__(origin.origin,
                                            query_uid=query_uid,
                                            match_uid=match_uid)

    def emit_entity(self, data):
        # Enrichment results are first held as inactive and become active only
        # once the judgement between the query and result entities is confirmed
        data['active'] = False
        entity = super(ResultEmitter, self).emit_entity(data)
        if self.judgement is None and entity.uid == self.match_uid:
            # Generate a tentative mapping.
            query = Entity.get(self.query_uid)
            Mapping.save(self.match_uid, self.query_uid,
                         None, score=query.compare(entity))
        session.commit()
        return entity

    def __repr__(self):
        return '<ResultEmitter(%r, %r, %r)>' % (self.origin,
                                                self.query_uid,
                                                self.match_uid)
