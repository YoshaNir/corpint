import logging
from corpint.load.util import get_uid
from corpint.integrate import score_pair, fingerprint_entity


class Origin(object):

    def __init__(self, project, name):
        self.project = project
        self.name = unicode(name)
        self.qname = '%s.%s' % (self.project.prefix, self.name)
        self.log = logging.getLogger(self.qname)

    def uid(self, *args):
        return get_uid(self.name, *args)

    def result(self, query_uid, match_uid):
        return Result(self, query_uid, match_uid)

    def emit_entity(self, data):
        data['origin'] = self.name
        self.project.emit_entity(data)

    def entity_exists(self, uid):
        return self.project.entities.find_one(uid=uid, origin=self.name)

    def emit_link(self, data):
        data['origin'] = self.name
        self.project.emit_link(data)

    def emit_judgement(self, uida, uidb, judgement, score=None, decided=False):
        self.project.emit_judgement(uida, uidb, judgement, score=score,
                                    decided=decided)

    def emit_document(self, url, title=None, uid=None, query=None,
                      publisher=None):
        self.project.emit_document(self.name, url, title=title, uid=uid,
                                   query=query, publisher=publisher)

    def clear(self):
        self.project.entities.delete(origin=self.name)
        self.project.aliases.delete(origin=self.name)
        self.project.links.delete(origin=self.name)

    def __repr__(self):
        return '<Origin(%r, %r)>' % (self.project, self.name)


class Result(object):

    def __init__(self, origin, query_uid, match_uid):
        self.origin = origin
        self.query_uid = query_uid
        self.match_uid = match_uid
        self.log = origin.log
        self._entities = origin.project.entities
        self.judgement = origin.project.get_judgement(match_uid, query_uid)

    def uid(self, *args):
        return self.origin.uid(*args)

    def entity_exists(self, uid):
        return self.origin.entity_exists(uid)

    def exists(self):
        data = self._entities.find_one(origin=self.origin.name,
                                       query_uid=self.query_uid,
                                       match_uid=self.match_uid)
        return data is not None

    def score_entity(self, data):
        if self.judgement is not None:
            return

        query = self.origin.project.get_entity(self.query_uid)
        query = fingerprint_entity(query)
        data = fingerprint_entity(data)
        score = score_pair(query, data)
        self.emit_judgement(self.match_uid, self.query_uid, None, score)

    def emit_entity(self, data):
        if self.judgement is False:
            return

        data['query_uid'] = self.query_uid
        data['match_uid'] = self.match_uid

        if self.match_uid == data.get('uid'):
            self.score_entity(data)
            data.pop('fps', None)

        self.origin.emit_entity(data)

    def emit_link(self, data):
        if self.judgement is False:
            return

        self.origin.emit_link(data)

    def emit_judgement(self, uida, uidb, judgement, score=None, decided=False):
        self.origin.emit_judgement(uida, uidb, judgement, score=score,
                                   decided=decided)

    def clear(self):
        for entity in self._entities.find(origin=self.origin.name,
                                          query_uid=self.query_uid,
                                          match_uid=self.match_uid):
            self.origin.project.delete_entity(entity.get('uid'))

    def __repr__(self):
        return '<Result(%r, %r, %r)>' % (self.origin, self.query_uid,
                                         self.match_uid)
