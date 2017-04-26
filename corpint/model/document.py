from hashlib import sha1
from dalet import parse_url
from normality import stringify, slugify
from sqlalchemy import Column, Unicode, Integer

from corpint.core import session, project
from corpint.model.common import Base, SchemaObject, UID_LENGTH


class Document(SchemaObject, Base):
    __tablename__ = 'document'

    id = Column(Integer, primary_key=True)
    project = Column(Unicode(), nullable=False)
    origin = Column(Unicode(), nullable=False)
    uid = Column(Unicode(UID_LENGTH), index=True, nullable=False)
    entity_uid = Column(Unicode(UID_LENGTH), index=True, nullable=False)
    title = Column(Unicode(), nullable=True)
    url = Column(Unicode(), nullable=True)
    publisher = Column(Unicode(), nullable=True)

    def delete(self):
        session.delete(self)

    @classmethod
    def save(cls, entity_uid, url, title, origin, uid=None, publisher=None):
        url = parse_url(url)
        if url is None:
            raise ValueError("No URL on document!")
        if entity_uid is None:
            raise ValueError("No UID on document: %r" % url)
        if uid is None:
            uid = unicode(sha1(url.encode('utf-8')).hexdigest())

        obj = cls.get(entity_uid, uid, origin)
        if obj is None:
            obj = cls()
            obj.project = project.name
            obj.origin = origin
            obj.entity_uid = entity_uid
            obj.uid = uid

        obj.url = url
        obj.title = stringify(title) or url
        obj.publisher = publisher
        session.add(obj)
        return obj

    @classmethod
    def get(cls, entity_uid, uid, origin):
        q = cls.find()
        q = q.filter(cls.entity_uid == entity_uid)
        q = q.filter(cls.uid == uid)
        q = q.filter(cls.origin == origin)
        return q.first()

    @classmethod
    def find(cls):
        q = session.query(cls)
        q = q.filter(cls.project == project.name)
        return q

    @classmethod
    def find_by_entity(cls, entity_uid):
        return cls.find().filter(cls.entity_uid == entity_uid)

    @classmethod
    def delete_by_entity(cls, entity_uid):
        cls.find_by_entity(entity_uid).delete()

    def __repr__(self):
        return '<Address(%r, %r)>' % (self.entity_uid, self.clean)
