from collections import defaultdict
from itertools import combinations
from sqlalchemy import Unicode
from dalet import parse_boolean
from unicodecsv import DictReader, DictWriter

from corpint.integrate.merge import merge_entities, merge_links  # noqa
from corpint.integrate.dupes import generate_candidates, score_pair  # noqa
from corpint.integrate.dupes import fingerprint_entity  # noqa
from corpint.integrate.util import normalize_name, get_clusters
from corpint.integrate.util import sorttuple, get_decided  # noqa
from corpint.util import ensure_column


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


def export_mappings(project, file_name):
    with open(file_name, 'w') as fh:
        writer = DictWriter(fh, fieldnames=['left_uid', 'right_uid',
                                            'judgement'])
        writer.writeheader()
        for mapping in project.mappings.find(decided=True):
            writer.writerow({
                'left_uid': mapping.get('left_uid'),
                'right_uid': mapping.get('right_uid'),
                'judgement': mapping.get('judgement')
            })


def import_mappings(project, file_name):
    with open(file_name, 'r') as fh:
        for row in DictReader(fh):
            judgement = parse_boolean(row.get('judgement'), None)
            score = None
            if judgement is None:
                left = project.get_entity(row.get('left_uid'))
                right = project.get_entity(row.get('left_uid'))
                if left is not None and right is not None:
                    score = score_pair(fingerprint_entity(left),
                                       fingerprint_entity(right))
            project.emit_judgement(row.get('left_uid'),
                                   row.get('right_uid'),
                                   judgement, score=score, decided=True)
