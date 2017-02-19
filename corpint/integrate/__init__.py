from collections import defaultdict
from itertools import combinations, product
from sqlalchemy import Unicode
import Levenshtein
import fingerprints

from corpint.integrate.merge import merge_entities, merge_links  # noqa
from corpint.integrate.util import normalize_name, get_clusters
from corpint.integrate.util import get_decided, merkle, sorttuple
from corpint.util import ensure_column
from corpint.schema import ASSET, PERSON


def name_merge(project, origins):
    by_name = defaultdict(set)
    for entity in project.entities:
        if len(origins) and entity['origin'] not in origins:
            continue
        name = normalize_name(entity['name'])
        by_name[name].add(entity['uid'])

    for name, uids in by_name.items():
        if len(uids) == 1:
            continue

        project.log.info("Merge: %s (%d matches)", name, len(uids))
        for (left, right) in combinations(uids, 2):
            project.emit_judgement(left, right, True)


def score_pair(left, right):
    types = right.get('type'), left.get('type')
    if ASSET in types:
        return 0

    score = 0
    for lfp, rfp in product(left['fps'], right['fps']):
        distance = Levenshtein.distance(lfp, rfp)
        lscore = 1 - (distance / float(max(len(lfp), len(rfp))))
        score = max(score, lscore)

    if PERSON not in types:
        score *= .85

    countries = right.get('country'), left.get('country')
    if None not in countries and len(set(countries)) != 1:
        score *= 0.9

    return score


def generate_candidates(project, origins=[], threshold=.5):
    origins = set(origins)
    project.log.info("Loading entities...")
    aliases = defaultdict(set)
    for alias in project.aliases:
        aliases[alias.get('uid')].add(alias.get('name'))

    entities = []
    for entity in project.entities:
        names = aliases.get(entity.get('uid'), set())
        names.add(entity.get('name'))
        fps = [fingerprints.generate(n) for n in names]
        fps = [fp for fp in fps if fp is not None]
        entity['fps'] = fps
        entities.append(entity)

    project.log.info("Loaded %s entities.", len(entities))
    decided = get_decided(project)
    project.log.info("Loaded %s decisions.", len(decided))
    project.mappings.delete(judgement=None)

    for (left, right) in combinations(entities, 2):
        origins_ = set((right.get('origin'), left.get('origin')))
        if len(origins) and origins.isdisjoint(origins_):
            continue

        left_uid, right_uid = left['uid'], right['uid']
        combo = sorttuple(left_uid, right_uid)
        if combo in decided:
            continue

        score = score_pair(left, right)
        if score <= threshold:
            continue

        project.log.info("Candidate [%.3f]: %s <-> %s",
                         score, left['name'], right['name'])
        project.emit_judgement(left_uid, right_uid,
                               judgement=None,
                               score=score)


def canonicalise(project):
    updates = (
        (project.entities, 'uid', 'uid_canonical'),
        (project.aliases, 'uid', 'uid_canonical'),
        (project.links, 'source', 'source_canonical'),
        (project.links, 'target', 'target_canonical'),
        (project.documents, 'uid', 'uid_canonical'),
    )

    for (table, src, dest) in updates:
        table.create_index([src])
        ensure_column(table, dest, Unicode)
        table.create_index([dest])
        project.db.query("UPDATE %s SET %s = %s;" % (table.table.name, dest, src))  # noqa

    clusters = get_clusters(project)
    project.log.info("Canonicalise: %d clusters", len(clusters))
    for uids in clusters:
        canon = merkle(uids)
        uids = ', '.join(["'%s'" % u for u in uids])
        for (table, src, dest) in updates:
            query = "UPDATE %s SET %s = '%s' WHERE %s IN (%s)"
            query = query % (table.table.name, dest, canon, src, uids)
            project.db.query(query)


def add_mapping_names(project):
    project.log.info("Setting names on mapping...")
    ensure_column(project.mappings, 'left_name', Unicode)
    ensure_column(project.mappings, 'right_name', Unicode)
    table = project.mappings.table
    for entity in project.entities:
        uid = entity.get('uid')
        name = entity.get('name').replace("'", '"')
        project.db.query("UPDATE %s SET left_name = '%s' WHERE left_uid = '%s';" % (table.name, name, uid))  # noqa
        project.db.query("UPDATE %s SET right_name = '%s' WHERE right_uid = '%s';" % (table.name, name, uid))  # noqa
