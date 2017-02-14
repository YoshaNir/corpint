from hashlib import sha1
from normality import normalize


def normalize_name(name):
    return normalize(name, ascii=True)


def sorttuple(a, b):
    """A sorted two-value tuple."""
    return (max(a, b), min(a, b))


def merkle(items):
    """Generate a hash of hashes."""
    uid = sha1()
    for item in sorted(set(items)):
        uid.update(unicode(item).encode('utf-8'))
    return uid.hexdigest()


def get_trained(project, judgement):
    """Find judgements of a particular type in the mappings table."""
    for mapping in project.mappings.find(judgement=judgement):
        yield sorttuple(mapping.get('left_uid'), mapping.get('right_uid'))


def get_clusters(project):
    """Get a list of identity clusters."""
    clusters = []
    for (a, b) in get_trained(project, True):
        for cluster in clusters:
            if a in cluster or b in cluster:
                cluster.add(a)
                cluster.add(b)
                break
        else:
            clusters.append(set([a, b]))
    return clusters


def get_same_as(project):
    """Get a list of identity mappings."""
    same_as = {}
    for cluster in get_clusters(project):
        for uid in cluster:
            same_as[uid] = cluster
    return same_as


def get_decided(project):
    "A set of sorted tuples representing all the entity/entity decisions made."
    decided = set()
    same_as = get_same_as(project)
    for uid, sames in same_as.items():
        for ouid in sames:
            decided.add(sorttuple(uid, ouid))

    for (a, b) in get_trained(project, False):
        for left in same_as.get(a, set([a])):
            for right in same_as.get(b, set([b])):
                decided.add(sorttuple(left, right))

    return decided
