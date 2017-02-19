from collections import defaultdict
from itertools import combinations, product
import Levenshtein
import fingerprints

from corpint.integrate.merge import merge_entities, merge_links  # noqa
from corpint.integrate.util import get_decided, sorttuple
from corpint.schema import ASSET, PERSON


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
