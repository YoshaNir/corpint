from collections import defaultdict
from itertools import combinations, product
import Levenshtein
import fingerprints

from corpint.integrate.merge import merge_entities, merge_links  # noqa
from corpint.integrate.util import get_decided, sorttuple
from corpint.schema import ASSET, PERSON

IDENTIFIERS = ['aleph_id', 'opencorporates_url', 'bvd_id']


def score_pair(left, right):
    for identifier in IDENTIFIERS:
        ids = right.get(identifier), left.get('identifier')
        if None not in ids and len(set(ids)) == 1:
            return 2.0

    types = right.get('type'), left.get('type')
    if ASSET in types:
        return 0

    score = 0
    for lfp, rfp in product(left['fps'], right['fps']):
        distance = Levenshtein.distance(lfp, rfp)
        lscore = 1 - (distance / float(max(len(lfp), len(rfp))))
        score = max(score, lscore)

    if PERSON not in types:
        score *= .9

    countries = right.get('country'), left.get('country')
    if None not in countries and len(set(countries)) != 1:
        score *= .9

    regnr = right.get('registration_number'), left.get('registration_number')
    if None not in regnr and len(set(regnr)) != 1:
        score *= .9

    if True not in (right.get('tasked'), left.get('tasked')):
        score *= .95

    return score


def fingerprint_entity(entity):
    aliases = entity.get('aliases') or []
    names = set(aliases)
    names.add(entity.get('name'))
    fps = [fingerprints.generate(n) for n in names]
    fps = [fp for fp in fps if fp is not None]
    entity['fps'] = fps
    return entity


def generate_candidates(project, origins=[], threshold=.5):
    origins = set(origins)
    project.log.info("Loading entities...")
    aliases = defaultdict(set)
    for alias in project.aliases:
        aliases[alias.get('uid')].add(alias.get('name'))

    entities = []
    for entity in project.entities:
        entity['aliases'] = aliases.get(entity.get('uid'), set())
        entities.append(fingerprint_entity(entity))

    project.log.info("Loaded %s entities.", len(entities))
    decided = get_decided(project)
    project.log.info("Loaded %s decisions.", len(decided))
    # project.mappings.delete(judgement=None)

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

        judgement = True if score > 1.0 else None
        project.log.info("Candidate [%.3f]: %s <-> %s",
                         score, left['name'], right['name'])
        project.emit_judgement(left_uid, right_uid,
                               judgement=judgement,
                               score=score)
