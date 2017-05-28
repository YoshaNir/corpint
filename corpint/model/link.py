from sqlalchemy import Column, Unicode, Integer
from sqlalchemy.dialects.postgresql import JSONB

from corpint.core import session, project
from corpint.model.common import Base, SchemaObject, UID_LENGTH


class Link(SchemaObject, Base):
    __tablename__ = 'link'

    id = Column(Integer, primary_key=True)
    source_uid = Column(Unicode(UID_LENGTH), index=True, nullable=False)
    source_canonical_uid = Column(Unicode(UID_LENGTH), nullable=True)
    target_uid = Column(Unicode(UID_LENGTH), index=True, nullable=False)
    target_canonical_uid = Column(Unicode(UID_LENGTH), nullable=True)
    project = Column(Unicode(255), index=True, nullable=False)
    origin = Column(Unicode(255), index=True, nullable=False)
    schema = Column(Unicode(255), nullable=True)
    data = Column(JSONB, default={})

    def delete(self):
        session.delete(self)

    @classmethod
    def save(cls, data, origin):
        source_uid = data.pop('source_uid', None)
        target_uid = data.pop('target_uid', None)
        if source_uid is None or target_uid is None:
            raise ValueError("No UID on link: %r" % data)
        obj = cls.get(source_uid, target_uid, origin=origin)
        if obj is None:
            obj = cls()
            obj.project = project.name
            obj.origin = origin
            obj.source_uid = source_uid
            obj.source_canonical_uid = source_uid
            obj.target_uid = target_uid
            obj.target_canonical_uid = target_uid
        obj.schema = data.pop('schema', None)
        obj.data = data
        session.add(obj)
        return obj

    @classmethod
    def get(cls, source_uid, target_uid, origin=None):
        q = cls.find()
        q = q.filter(cls.source_uid == source_uid)
        q = q.filter(cls.target_uid == target_uid)
        if origin is not None:
            q = q.filter(cls.origin == origin)
        return q.first()

    @classmethod
    def find(cls):
        q = session.query(cls)
        q = q.filter(cls.project == project.name)
        return q

    def __repr__(self):
        return '<Link(%r, %r)>' % (self.source_uid, self.target_uid)
