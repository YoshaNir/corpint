from itertools import chain, combinations
from sqlalchemy import Column, Unicode, Boolean, Float

from corpint.core import session, project
from corpint.model.entity import Entity
from corpint.model.link import Link
from corpint.model.common import Base, UID_LENGTH


class Mapping(Base):
    __tablename__ = 'mapping'

    project = Column(Unicode(255), index=True, nullable=False)
    left_uid = Column(Unicode(UID_LENGTH), index=True, primary_key=True)
    right_uid = Column(Unicode(UID_LENGTH), index=True, primary_key=True)
    judgement = Column(Boolean, default=None, nullable=True)
    decided = Column(Boolean, default=False)
    generated = Column(Boolean, default=False)
    score = Column(Float, default=None, nullable=True)

    @property
    def left(self):
        if not hasattr(self, '_left'):
            self._left = Entity.get(self.left_uid)
        return self._left

    @property
    def right(self):
        if not hasattr(self, '_right'):
            self._right = Entity.get(self.right_uid)
        return self._right

    def delete(self):
        session.delete(self)

    @classmethod
    def save(cls, uida, uidb, judgement, decided=False, generated=False,
             score=None):
        left_uid, right_uid = cls.sort_uids(uida, uidb)
        obj = cls.get(left_uid, right_uid)
        if obj is None:
            obj = cls()
            obj.project = project.name
            obj.left_uid = left_uid
            obj.right_uid = right_uid
        obj.judgement = judgement
        if judgement is not None:
            decided = True
        obj.generated = generated
        obj.decided = decided or obj.decided
        if score is not None:
            score = float(score)
        obj.score = score
        session.add(obj)

        entities = chain(
            Entity.find_by_result(left_uid, right_uid),
            Entity.find_by_result(right_uid, left_uid)
        )
        for entity in entities:
            if judgement is False:
                # Clear out rejected results.
                entity.delete()
            else:
                # Set entities to enabled.
                entity.active = True
        return obj

    @classmethod
    def get(cls, uida, uidb):
        """Load a mapping by it's end points."""
        left_uid, right_uid = cls.sort_uids(uida, uidb)
        q = session.query(cls)
        q = q.filter(cls.project == project.name)
        q = q.filter(cls.left_uid == left_uid)
        q = q.filter(cls.right_uid == right_uid)
        return q.first()

    @classmethod
    def get_judgement(cls, uida, uidb):
        """Load a judgement, or return None if not in the DB."""
        mapping = cls.get(uida, uidb)
        if mapping is not None:
            return mapping.judgement

    @classmethod
    def sort_uids(cls, uida, uidb):
        """Get two entity IDs into a standard order."""
        return (max(uida, uidb), min(uida, uidb))

    @classmethod
    def find_judgements(cls, judgement):
        """Find entity IDs linked by judgements of a particular type."""
        q = session.query(cls.left_uid, cls.right_uid)
        q = q.filter(cls.project == project.name)
        q = q.filter(cls.judgement == judgement)
        for (uida, uidb) in q:
            yield cls.sort_uids(uida, uidb)

    @classmethod
    def find_decided(cls):
        """Find entity IDs linked by judgements of a particular type."""
        q = session.query(cls)
        q = q.filter(cls.project == project.name)
        q = q.filter(cls.decided == True)  # noqa
        return q

    @classmethod
    def generate_clusters(cls):
        """Get a list of lists of entities which are identical."""
        clusters = []
        for pair in cls.find_judgements(True):
            new_clusters = []
            cluster = set(pair)
            for cand in clusters:
                if cluster.isdisjoint(cand):
                    new_clusters.append(cand)
                else:
                    cluster = cluster.union(cand)
            new_clusters.append(cluster)
            clusters = new_clusters
        return clusters

    @classmethod
    def get_decided(cls):
        """A set of sorted tuples representing all the decisions made.
        This is used to avoid prompting for matches that can already be
        inferred transitively."""
        decided = set()
        same_as = {}
        for cluster in cls.generate_clusters():
            for uid in cluster:
                same_as[uid] = cluster
                for other in cluster:
                    decided.add(cls.sort_uids(uid, other))

        for (a, b) in cls.find_judgements(False):
            for left in same_as.get(a, [a]):
                for right in same_as.get(b, [b]):
                    decided.add(cls.sort_uids(left, right))

        return decided

    @classmethod
    def generate_scored_mappings(cls, origins=[], threshold=.5):
        """Do a cross-product comparison of entities and generate mappings."""
        q = Entity.find_by_origins(origins)
        entities = q.filter(Entity.active == True).all()  # noqa
        decided = cls.get_decided()
        project.log.info("Loaded: %s entities, %s decisions.",
                         len(entities), len(decided))

        for (left, right) in combinations(entities, 2):
            if cls.sort_uids(left.uid, right.uid) in decided:
                continue

            score = left.compare(right)
            if score <= threshold:
                continue

            judgement = True if score > 1.0 else None
            project.log.info("Candidate [%.3f]: %s <-> %s",
                             score, left.name, right.name)
            cls.save(left.uid, right.uid, judgement,
                     score=score, generated=True)
            session.commit()

    @classmethod
    def cleanup(cls):
        """Delete all undecided mappings."""
        q = session.query(cls)
        q = q.filter(cls.project == project.name)
        q = q.filter(cls.decided == False)  # noqa
        q = q.filter(cls.generated == True)  # noqa
        q.delete(synchronize_session='fetch')

    @classmethod
    def canonicalize(cls):
        """Write out canonical_uids based on entity mappings."""
        q = session.query(Entity)
        q = q.filter(Entity.project == project.name)
        q.update({Entity.canonical_uid: Entity.uid},
                 synchronize_session='fetch')

        q = session.query(Link)
        q = q.filter(Link.project == project.name)
        q.update({Link.source_canonical_uid: Link.source_uid},
                 synchronize_session='fetch')
        q.update({Link.target_canonical_uid: Link.target_uid},
                 synchronize_session='fetch')

        clusters = cls.generate_clusters()
        project.log.info("Canonicalize: %d clusters", len(clusters))
        for uids in clusters:
            canonical_uid = max(uids)
            q = session.query(Entity)
            q = q.filter(Entity.project == project.name)
            q = q.filter(Entity.uid.in_(uids))
            q.update({Entity.canonical_uid: canonical_uid},
                     synchronize_session='fetch')

            q = session.query(Link)
            q = q.filter(Link.project == project.name)
            q = q.filter(Link.source_uid.in_(uids))
            q.update({Link.source_canonical_uid: canonical_uid},
                     synchronize_session='fetch')
            q = session.query(Link)
            q = q.filter(Link.project == project.name)
            q = q.filter(Link.target_uid.in_(uids))
            q.update({Link.target_canonical_uid: canonical_uid},
                     synchronize_session='fetch')

    @classmethod
    def find_undecided(cls, limit=10, offset=0):
        """Return candidates for manual matching."""
        decided = cls.get_decided()
        q = session.query(cls)
        q = q.filter(cls.project == project.name)
        q = q.filter(cls.decided == False)  # noqa
        q = q.filter(cls.judgement == None)  # noqa
        q = q.order_by(cls.score.desc())
        q = q.offset(offset)
        mappings = []
        for mapping in q.yield_per(limit):
            if (mapping.left_uid, mapping.right_uid) in decided or \
               mapping.left is None or mapping.right is None:
                mapping.delete()
                continue
            mappings.append(mapping)
            if len(mappings) == limit:
                break
        session.commit()
        return mappings

    def __repr__(self):
        return '<Mapping(%r, %r, %r)>' % (self.left_uid, self.right_uid,
                                          self.judgement)
