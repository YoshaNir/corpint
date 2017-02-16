from pprint import pprint  # noqa
from collections import defaultdict
import Levenshtein

from corpint.integrate.util import sorttuple
from corpint.schema import choose_best_type


def choose_best_name(values):
    values = [v.strip() for v in values if v is not None and len(v.strip())]
    if len(values):
        return Levenshtein.median(values)

def merge_values(values):
    return '; '.join(set([unicode(v) for v in values]))


def merge_entity(project, uid_canonical):
    if uid_canonical is None:
        return
    entity = defaultdict(list)
    aliases = set()

    for alias in project.aliases.find(uid_canonical=uid_canonical):
        if alias.get('uid') is None:
            continue
        aliases.add(alias['name'])

    for part in project.entities.find(uid_canonical=uid_canonical):
        if part.get('uid') is None:
            continue
        for key, value in part.items():
            if key in ['uid_canonical', 'id']:
                continue
            if value is None or not len(unicode(value).strip()):
                continue
            entity[key].append(value)

    merged = {
        'uid': set(),
        'origin': set(),
        'address': set()
    }
    for key, values in entity.items():
        if key in ['uid', 'origin', 'address', 'publisher']:
            merged[key].update(values)
            continue
        if key == 'type':
            value = choose_best_type(values)
        elif key == 'weight':
            value = max(values)
        elif key == 'name':
            value = choose_best_name(values)
            aliases.update(values)
            if value in aliases:
                aliases.remove(value)
        else:
            value = merge_values(values)
        if value is None:
            continue
        merged[key] = value
    # project.log.info("Merged: %(name)s", merged)
    merged['uid_parts'] = merged['uid']
    merged['uid'] = uid_canonical
    merged['type'] = merged.get('type')
    merged['aliases'] = aliases
    merged['names'] = set(aliases)
    if 'name' in merged:
        merged['names'].add(merged['name'])
    return merged


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
