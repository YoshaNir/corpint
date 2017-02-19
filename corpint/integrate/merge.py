from pprint import pprint  # noqa
from collections import defaultdict
import Levenshtein

from corpint.integrate.util import sorttuple
from corpint.schema import choose_best_type, OTHER

MULTI_FIELDS = ['uid', 'origin', 'address', 'address_canonical', 'publisher']


def choose_best_name(values):
    values = [v.strip() for v in values if v is not None and len(v.strip())]
    if len(values):
        return Levenshtein.setmedian(values)


def merge_values(values):
    return '; '.join(set([unicode(v) for v in values]))


def merge_entity(project, uid_canonical):
    if uid_canonical is None:
        return

    aliases = list()
    for alias in project.aliases.find(uid_canonical=uid_canonical):
        if alias.get('uid') is None:
            continue
        aliases.append(alias['name'])

    entity = defaultdict(list)
    for key in MULTI_FIELDS:
        entity[key] = []

    for part in project.entities.find(uid_canonical=uid_canonical):
        if part.get('uid') is None:
            continue
        part.pop('id', None)
        part.pop('uid_canonical', None)
        aliases.append(part.pop('name', None))
        entity['type'] = choose_best_type((entity.pop('type', OTHER),
                                           part.pop('type', OTHER)))
        entity['weight'] = max(entity.get('weight'), 0,
                               int(part.pop('weight', 0)))
        entity['lat'] = part.pop('lat') or entity.get('lat')
        entity['lng'] = part.pop('lng') or entity.get('lng')

        for key, value in part.items():
            if value is None:
                continue
            value = unicode(value).strip()
            if not len(value):
                continue
            entity[key].append(value)

    for key, value in entity.items():
        if key in MULTI_FIELDS:
            entity[key] = set(value)
            continue

        if value is None:
            continue

        if isinstance(value, list):
            value = merge_values(value)

        entity[key] = value

    aliases = [a.strip() for a in aliases
               if a is not None and len(a.strip())]
    entity['name'] = choose_best_name(aliases)
    aliases = set(aliases)
    entity['names'] = set(aliases)
    if entity['name'] in aliases:
        aliases.remove(entity['name'])
    entity['aliases'] = aliases
    entity['uid_parts'] = entity['uid']
    entity['uid'] = uid_canonical
    if 'weight' not in entity or not len(entity['uid_parts']):
        return None
    entity = dict(entity)
    # pprint(entity)
    return entity


def merge_entities(project):
    for row in project.entities.distinct('uid_canonical'):
        entity = merge_entity(project, row.get('uid_canonical'))
        if entity is not None:
            yield entity


def merge_links(project):
    links = defaultdict(list)
    for link in project.links:
        link.pop('source')
        link.pop('target')
        nodes = sorttuple(link.pop('source_canonical'),
                          link.pop('target_canonical'))
        if None in nodes:
            continue
        links[nodes].append(dict(link))

    for (source, target), items in links.items():
        merged = {
            'source': source,
            'target': target,
            'origin': set()
        }
        link = defaultdict(list)
        for item in items:
            item.pop('id')
            for key, value in item.items():
                if value is None or not len(unicode(value).strip()):
                    continue
                link[key].append(value)

        for key, values in link.items():
            if key == 'origin':
                merged[key].update(values)
                continue
            else:
                value = merge_values(values)
            if value is None:
                continue
            merged[key] = value

        # pprint(merged)
        yield merged
