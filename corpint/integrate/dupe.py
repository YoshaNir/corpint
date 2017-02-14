# import fingerprints
from normality import ascii_text
from sqlalchemy import Unicode
from pprint import pprint  # noqa
import dedupe

from corpint.schema import TYPES
from corpint.integrate.util import sorttuple

NTYPES = [t for t in TYPES if t is not None]
VARIABLES = [
    {'field': 'uid', 'type': 'Exists', 'has missing': True},
    {
        'field': 'name',
        'type': 'String',
        'has missing': False,
        'crf': True,
        # 'variable name': 'name'
    },
    {
        'field': 'type',
        'type': 'Exact',
        # 'type': 'Categorical',
        'has missing': True,
        # 'categories': NTYPES,
        # 'variable name': 'type'
    },
    {'field': 'date', 'type': 'ShortString', 'has missing': True},
    {'field': 'country', 'type': 'Exact', 'has missing': True},
    {'field': 'origin', 'type': 'Exact', 'has missing': False},
    {'field': 'address', 'type': 'Address', 'has missing': True},
    # {'type': 'Interaction', 'interaction variables': ['name', 'type']}
]


def strconv(text):
    if text is None or not len(text.strip()):
        return
    return ascii_text(text)


def to_record(entity):
    # fp = fingerprints.generate(entity.get('name'))
    date = entity.get('incorporation_date') or entity.get('dob')
    return {
        'uid': strconv(entity.get('uid')),
        'name': strconv(entity.get('name')),
        'type': strconv(entity.get('type')),
        'origin': entity.get('origin'),
        'date': strconv(date),
        'country': strconv(entity.get('country')),
        'address': strconv(entity.get('address'))
    }


def get_trainset(project, judgement, data):
    trainset = []
    for mapping in project.mappings.find(judgement=judgement, trained=True):
        uida, uidb = sorttuple(mapping.get('left_uid'), mapping.get('right_uid'))
        enta = data.get(uida)
        entb = data.get(uidb)
        if enta is not None and entb is not None:
            trainset.append((enta, entb))
    return trainset


def create_deduper(project):
    deduper = dedupe.Dedupe(VARIABLES, num_cores=4)
    data = {e['uid']: to_record(e) for e in project.entities}
    if len(data):
        deduper.sample(data)
        deduper.markPairs({
            'match': get_trainset(project, True, data),
            'distinct': get_trainset(project, False, data)
        })
    try:
        deduper.train()
    except ValueError as verr:
        project.log.error("Cannot train deduper: %r", verr)
        return
    return deduper, data


def train_judgement(project, deduper, uida, uidb, judgement):
    if judgement is None:
        return
    enta = project.entities.find_one(uid=uida)
    entb = project.entities.find_one(uid=uidb)
    pair = (to_record(enta), to_record(entb))
    match, distinct = [], []
    if judgement:
        match.append(pair)
    else:
        distinct.append(pair)
    deduper.markPairs({'match': match, 'distinct': distinct})


def pairwise_score(project, deduper, enta, entb):
    block = (
        (enta.get('uid'), enta, set([])),
        (entb.get('uid'), entb, set([]))
    )
    clusters = deduper.matchBlocks([block], threshold=.1)
    for (_, scores) in clusters:
        return min(scores)
    return None


# def canonicalise(project):
#     updates = (
#         (project.entities, 'uid', 'uid_canonical'),
#         (project.aliases, 'uid', 'uid_canonical'),
#         (project.links, 'source', 'source_canonical'),
#         (project.links, 'target', 'target_canonical'),
#     )

#     for (table, src, dest) in updates:
#         table.create_index([src])
#         ensure_column(table, dest, Unicode)
#         table.create_index([dest])
#         project.db.query("UPDATE %s SET %s = %s;" % (table.table.name, dest, src))  # noqa


#     threshold = deduper.threshold(data, recall_weight=1)
#     # threshold = min(.8, threshold)
#     blocks = deduper.match(data, threshold)
#     project.log.info("%d clusters at threshold: %.4f", len(blocks), threshold)
#     for (uids, scores) in blocks:
#         canon = merkle(uids)
#         uids = ', '.join(["'%s'" % u for u in uids])
#         for (table, src, dest) in updates:
#             query = "UPDATE %s SET %s = '%s' WHERE %s IN (%s)"
#             query = query % (table.table.name, dest, canon, src, uids)
#             project.db.query(query)
