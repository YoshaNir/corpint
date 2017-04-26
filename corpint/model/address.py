from normality import stringify, slugify
from dalet import clean_address
from sqlalchemy import Column, Unicode, Integer, Float

from corpint.core import session, project
from corpint.model.common import Base, SchemaObject, UID_LENGTH


class Address(SchemaObject, Base):
    __tablename__ = 'address'

    id = Column(Integer, primary_key=True)
    project = Column(Unicode(), nullable=False)
    origin = Column(Unicode(), nullable=False)
    entity_uid = Column(Unicode(UID_LENGTH), index=True, nullable=False)
    address = Column(Unicode(), nullable=False)
    slug = Column(Unicode(), nullable=True)
    normalized = Column(Unicode(), nullable=True)
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)

    @property
    def clean(self):
        if not hasattr(self, '_clean'):
            self._clean = clean_address(self.address)
        return self._clean

    @property
    def display_label(self):
        address = self.normalized or self.address
        return clean_address(address)

    @property
    def display_slug(self):
        return slugify(self.display_label, sep=' ')

    def delete(self):
        session.delete(self)

    def update(self, normalized, latitude, longitude):
        q = session.query(Address)
        q = q.filter(Address.project == project.name)
        q = q.filter(Address.slug == self.slug)
        q.update({
            Address.normalized: normalized,
            Address.latitude: latitude,
            Address.longitude: longitude,
        }, synchronize_session='fetch')

    @classmethod
    def save(cls, entity_uid, address, origin):
        if entity_uid is None:
            raise ValueError("No UID on address: %r" % address)
        address = stringify(address)
        if address is None:
            return
        obj = cls.get(entity_uid, address, origin=origin)
        if obj is None:
            obj = cls()
            obj.project = project.name
            obj.origin = origin
            obj.entity_uid = entity_uid
            obj.address = address
        obj.slug = slugify(obj.clean, sep=' ')
        session.add(obj)
        return obj

    @classmethod
    def get(cls, entity_uid, address, origin=None):
        q = cls.find()
        q = q.filter(cls.entity_uid == entity_uid)
        q = q.filter(cls.address == address)
        if origin is not None:
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
