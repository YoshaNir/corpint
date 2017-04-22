from itertools import chain
from sqlalchemy import Column, Unicode, Boolean, Float

from corpint.model.entity import Entity
from corpint.model.common import Base, UID_LENGTH


class Mapping(Base):
    __tablename__ = 'mapping'

    project = Column(Unicode(255), index=True, nullable=False)
    left_uid = Column(Unicode(UID_LENGTH), index=True, nullable=False)
    right_uid = Column(Unicode(UID_LENGTH), index=True, nullable=False)
    judgement = Column(Boolean, default=None, nullable=True)
    decided = Column(Boolean, default=False)
    score = Column(Float, default=None, nullable=True)

    @classmethod
    def save(cls, project, uida, uidb, judgement, decided=False, score=None):
        left_uid, right_uid = cls.sort_uids(uida, uidb)
        obj = cls.get(project, left_uid, right_uid)
        if obj is None:
            obj = cls()
            obj.project = project.name
            obj.left_uid = left_uid
            obj.right_uid = right_uid
        obj.judgement = judgement
        obj.decided = decided
        if score is not None:
            score = float(score)
        obj.score = score
        project.session.add(obj)

        entities = chain(
            Entity.find_by_result(left_uid, right_uid),
            Entity.find_by_result(right_uid, left_uid)
        )
        for entity in entities:
            if judgement is False:
                # Clear out rejected results.
                entity.delete(project)
            else:
                # Set entities to enabled.
                entity.active = True
        return obj

    @classmethod
    def get(cls, project, uida, uidb):
        """Load a mapping by it's end points."""
        left_uid, right_uid = cls.sort_uids(uida, uidb)
        q = project.session.query(cls)
        q = q.filter(cls.project == project.name)
        q = q.filter(cls.left_uid == left_uid)
        q = q.filter(cls.right_uid == right_uid)
        return q.first()

    @classmethod
    def get_judgement(cls, project, uida, uidb):
        """Load a judgement, or return None if not in the DB."""
        mapping = cls.get(project, uida, uidb)
        if mapping is not None:
            return mapping.judgement

    @classmethod
    def sort_uids(cls, uida, uidb):
        """Get two entity IDs into a standard order."""
        return (max(uida, uidb), min(uida, uidb))

    @classmethod
    def find_judgements(cls, project, judgement):
        """Find entity IDs linked by judgements of a particular type."""
        q = project.session.query(cls.left_uid, cls.right_uid)
        q = q.filter(cls.project == project.name)
        q = q.filter(cls.judgement == judgement)
        for (uida, uidb) in q:
            yield cls.sort_uids(uida, uidb)

    @classmethod
    def generate_clusters(cls, project):
        """Get a list of lists of entities which are identical."""
        clusters = []
        for pair in cls.find_judgements(project, True):
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
    def get_decided(cls, project):
        """A set of sorted tuples representing all the decisions made.
        This is used to avoid prompting for matches that can already be
        inferred transitively."""
        decided = set()
        same_as = {}
        for cluster in cls.generate_clusters(project):
            for uid in cluster:
                same_as[uid] = cluster
                for other in cluster:
                    decided.add(cls.sort_uids(uid, other))

        for (a, b) in cls.find_judgements(project, False):
            for left in same_as.get(a, [a]):
                for right in same_as.get(b, [b]):
                    decided.add(cls.sort_uids(left, right))

        return decided

    @classmethod
    def canonicalize(cls, project):
        """Write out canonical_uids based on entity mappings."""
        q = project.session.query(Entity)
        q = q.filter(Entity.project == project.name)
        q.update({Entity.canonical_uid: Entity.uid},
                 synchronize_session='fetch')
        clusters = cls.generate_clusters(project)
        project.log.info("Canonicalise: %d clusters", len(clusters))
        for uids in clusters:
            canonical_uid = max(uids)
            q = project.session.query(Entity)
            q = q.filter(Entity.project == project.name)
            q = q.filter(Entity.uid._in(uids))
            q.update({Entity.canonical_uid: canonical_uid},
                     synchronize_session='fetch')

    def __repr__(self):
        return '<Mapping(%r, %r, %r)>' % (self.left_uid, self.right_uid,
                                          self.judgement)
