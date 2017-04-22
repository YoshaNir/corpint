from sqlalchemy import Column, Unicode, Boolean
from sqlalchemy.dialects.postgresql import JSONB
from dalet import parse_country, parse_boolean

from corpint.model.common import Base, UID_LENGTH
from corpint.model.schema import TYPES


class EntityCore(object):
    pass


class Entity(EntityCore, Base):
    __tablename__ = 'entity'

    project = Column(Unicode(255), index=True, nullable=False)
    origin = Column(Unicode(255), index=True, nullable=False)
    uid = Column(Unicode(UID_LENGTH), index=True, nullable=False)
    canonical_uid = Column(Unicode(UID_LENGTH), index=True, nullable=True)
    query_uid = Column(Unicode(UID_LENGTH), index=True, nullable=True)
    match_uid = Column(Unicode(UID_LENGTH), index=True, nullable=True)
    schema = Column(Unicode(255), nullable=True)
    tasked = Column(Boolean, default=False)
    active = Column(Boolean, default=True)
    data = Column(JSONB, default={})

    def delete(self, project):
        # Keeping the mappings.
        project.session.delete(self)
        # TODO: links

    @classmethod
    def save(cls, project, data, origin, query_uid=None, match_uid=None):
        uid = data.pop('uid', None)
        if uid is None:
            raise ValueError("No UID on entity: %r" % data)
        obj = cls.get(project, uid, query_uid, match_uid)
        if obj is None:
            obj = cls()
            obj.project = project.name
            obj.uid = uid
            obj.canonical_uid = uid
            obj.query_uid = query_uid
            obj.match_uid = match_uid
        obj.origin = origin
        obj.schema = data.get('schema')
        if obj.schema not in TYPES:
            raise ValueError("Invalid entity type: %r", data)

        obj.tasked = parse_boolean(data.get('tasked'), default=False)
        obj.active = parse_boolean(data.get('active'), default=True)
        project.session.add(obj)
        return obj

    @classmethod
    def get(cls, project, uid, query_uid, match_uid):
        q = project.session.query(cls)
        q = q.filter(cls.project == project.name)
        q = q.filter(cls.uid == uid)
        q = q.filter(cls.query_uid == query_uid)
        q = q.filter(cls.match_uid == match_uid)
        return q.first()

    @classmethod
    def find_by_result(cls, project, query_uid, match_uid):
        q = project.session.query(cls)
        q = q.filter(cls.project == project.name)
        q = q.filter(cls.query_uid == query_uid)
        q = q.filter(cls.match_uid == match_uid)
        return q

    @classmethod
    def delete_by_origin(cls, project, origin, query_uid=None, match_uid=None):
        q = project.session.query(cls)
        q = q.filter(cls.project == project.name)
        q = q.filter(cls.origin == origin)
        if query_uid is not None and match_uid is not None:
            q = q.filter(cls.query_uid == query_uid)
            q = q.filter(cls.match_uid == match_uid)
        for entity in q:
            q.delete(project)

    def __repr__(self):
        return '<Entity(%r)>' % self.uid
