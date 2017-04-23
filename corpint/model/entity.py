import Levenshtein
import fingerprints
from sqlalchemy import Column, Unicode, Boolean, Integer
from sqlalchemy.dialects.postgresql import JSONB
from itertools import product
from collections import Counter, defaultdict
from dalet import parse_boolean

from corpint.core import session, project
from corpint.model.common import Base, SchemaObject, UID_LENGTH
from corpint.model.schema import choose_best_schema
from corpint.model.schema import TYPES, ASSET, PERSON

IDENTIFIERS = ['aleph_id', 'opencorporates_url', 'bvd_id', 'wikidata_id']


class EntityCore(SchemaObject):

    @property
    def name(self):
        return self.data.get('name')

    @property
    def country(self):
        country = self.data.get('country')
        if country is not None:
            country = country.upper()
        return country

    @property
    def names(self):
        names = set(self.data.get('aliases', []))
        names.add(self.name)
        return names

    @property
    def fingerprints(self):
        if not hasattr(self, '_fingerprints'):
            self._fingerprints = set()
            for name in self.names:
                fp = fingerprints.generate(name)
                if fp is not None:
                    self._fingerprints.add(fp)
        return self._fingerprints

    def compare(self, other):
        for identifier in IDENTIFIERS:
            ids = self.data.get(identifier), other.data.get('identifier')
            if None not in ids and len(set(ids)) == 1:
                return 2.0

        schemata = self.schema, other.schema
        if ASSET in schemata:
            return 0

        score = 0
        for lfp, rfp in product(self.fingerprints, other.fingerprints):
            distance = Levenshtein.distance(lfp, rfp)
            lscore = 1 - (distance / float(max(len(lfp), len(rfp))))
            score = max(score, lscore)

        if PERSON not in schemata:
            score *= .95

        countries = self.country, other.country
        if None in countries or len(set(countries)) != 1:
            score *= .95

        regnr = (self.data.get('registration_number'),
                 other.data.get('registration_number'))
        if None not in regnr and len(set(regnr)) == 1:
            score *= 1.1

        if not self.tasked and not other.tasked:
            score *= .95

        return min(1.0, score)


class CompositeEntity(EntityCore):

    def __init__(self, entities):
        self.entities = tuple(entities)
        self.active = True
        self.uids = [e.uid for e in self.entities]
        self.uid = max([e.canonical_uid for e in self.entities])
        self.tasked = max([e.tasked for e in self.entities])
        self.schema = choose_best_schema([e.schema for e in self.entities])
        self.data = self._combine_data([e.data for e in self.entities])

    def _combine_data(self, components):
        data = defaultdict(list)
        names = []
        for part in components:
            names.append(part.pop('name', None))
            names.extend(part.get('aliases', []))
            for field, value in part.items():
                if field in self.MULTI:
                    data[field].extend(value)
                else:
                    data[field].append(value)

        for field, values in data.items():
            if field in self.MULTI:
                data[field] = list(set(values))
            else:
                # TODO: or use ';'?
                data[field] = Counter(values).most_common(1)[0][0]

        names = [n for n in names if n is not None]
        if len(names):
            data['name'] = Levenshtein.setmedian(names)
        return data

    def __repr__(self):
        return '<CompositeEntity(%r)>' % (self.entities,)


class Entity(EntityCore, Base):
    __tablename__ = 'entity'

    id = Column(Integer, primary_key=True)
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

    def delete(self):
        # Keeping the mappings.
        session.delete(self)
        # TODO: links

    @classmethod
    def save(cls, data, origin, query_uid=None, match_uid=None):
        uid = data.pop('uid', None)
        if uid is None:
            raise ValueError("No UID on entity: %r" % data)
        obj = cls.get(uid, query_uid=query_uid, match_uid=match_uid)
        if obj is None:
            obj = cls()
            obj.project = project.name
            obj.uid = uid
            obj.canonical_uid = uid
            obj.query_uid = query_uid
            obj.match_uid = match_uid
        obj.origin = origin
        obj.schema = data.pop('schema', None)
        if obj.schema not in TYPES:
            raise ValueError("Invalid entity type: %r", data)

        obj.tasked = parse_boolean(data.get('tasked'), default=False)
        obj.active = parse_boolean(data.get('active'), default=True)
        obj.data = obj.parse_data(data)
        session.add(obj)
        return obj

    @classmethod
    def get(cls, uid, query_uid=None, match_uid=None):
        q = cls.find_by_result(query_uid=query_uid, match_uid=match_uid)
        q = q.filter(cls.uid == uid)
        return q.first()

    @classmethod
    def find_by_result(cls, query_uid=None, match_uid=None):
        q = session.query(cls)
        q = q.filter(cls.project == project.name)
        if query_uid is not None and match_uid is not None:
            q = q.filter(cls.query_uid == query_uid)
            q = q.filter(cls.match_uid == match_uid)
        return q

    @classmethod
    def find_by_origins(cls, origins):
        q = session.query(cls)
        q = q.filter(cls.project == project.name)
        if len(origins):
            q = q.filter(cls.origin.in_(origins))
        return q

    @classmethod
    def delete_by_origin(cls, origin, query_uid=None, match_uid=None):
        q = cls.find_by_origins([origin])
        if query_uid is not None and match_uid is not None:
            q = q.filter(cls.query_uid == query_uid)
            q = q.filter(cls.match_uid == match_uid)
        for entity in q:
            entity.delete()

    @classmethod
    def iter_composite(cls, origins=[], tasked=None):
        sq = session.query(cls.canonical_uid.distinct())
        sq = sq.filter(cls.project == project.name)
        sq = sq.filter(cls.active == True)  # noqa
        if len(origins):
            sq = sq.filter(cls.origin.in_(origins))
        if tasked is not None:
            sq = sq.filter(cls.tasked == tasked)
        q = session.query(cls)
        q = q.filter(cls.project == project.name)
        q = q.filter(cls.active == True)  # noqa
        q = q.filter(cls.canonical_uid.in_(sq))
        q = q.order_by(cls.canonical_uid.asc())
        entities = []
        canonical_uid = None
        for entity in q:
            if entity.canonical_uid != canonical_uid:
                if len(entities):
                    yield CompositeEntity(entities)
                entities = []
            entities.append(entity)
            canonical_uid = entity.canonical_uid
        if len(entities):
            yield CompositeEntity(entities)

    def __repr__(self):
        return '<Entity(%r)>' % self.uid
