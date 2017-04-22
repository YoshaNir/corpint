import logging
import dataset
import urlnorm
from dalet import parse_country, parse_boolean  # noqa
from normality import stringify
from sqlalchemy import Boolean, Unicode, Float
from pprint import pprint  # noqa

from corpint.origin import Origin
from corpint.schema import TYPES
from corpint.integrate import canonicalise
from corpint.integrate import merge_entities, merge_links
from corpint.load.util import get_uid
from corpint.enrich import get_enrichers
from corpint.util import ensure_column

log = logging.getLogger(__name__)


class Project(object):

    def __init__(self, prefix, database_uri):
        self.prefix = unicode(prefix)
        self.log = logging.getLogger(self.prefix)
        self.db = dataset.connect(database_uri)
        self.entities = self.db['%s_entities' % self.prefix]
        self.aliases = self.db['%s_aliases' % self.prefix]
        self.links = self.db['%s_links' % self.prefix]
        self.mappings = self.db['%s_mappings' % self.prefix]
        self.documents = self.db['%s_documents' % self.prefix]

        ensure_column(self.mappings, 'judgement', Boolean)
        ensure_column(self.mappings, 'decided', Boolean)
        ensure_column(self.mappings, 'score', Float)
        ensure_column(self.mappings, 'left_uid', Unicode)
        ensure_column(self.mappings, 'right_uid', Unicode)
        ensure_column(self.documents, 'uid', Unicode)
        ensure_column(self.entities, 'uid', Unicode)
        ensure_column(self.entities, 'query_uid', Unicode)
        ensure_column(self.entities, 'match_uid', Unicode)
        ensure_column(self.links, 'source', Unicode)
        ensure_column(self.links, 'source_canonical', Unicode)
        ensure_column(self.links, 'target', Unicode)
        ensure_column(self.links, 'target_canonical', Unicode)

    def origin(self, name):
        return Origin(self, name)

    def emit_entity(self, data):
        data.pop('id', None)
        uid = data.get('uid')
        if uid is None:
            raise ValueError("No UID for entity: %r", data)
        data['uid_canonical'] = data.get('uid_canonical') or uid

        if data.get('type') not in TYPES:
            raise ValueError("Invalid entity type: %r", data)

        data['country'] = parse_country(data.get('country'))
        data['tasked'] = parse_boolean(data.get('tasked'))
        data['name'] = stringify(data.get('name'))
        data['match_uid'] = data.get('match_uid')
        data['query_uid'] = data.get('query_uid')

        for k, v in data.items():
            if k == 'aliases':
                continue
            data[k] = stringify(v)
            if data[k] is None:
                data.pop(k)

        # TODO: partial dates
        aliases = data.pop('aliases', [])
        self.entities.upsert(data, ['origin', 'uid', 'match_uid', 'query_uid'])
        for alias in aliases:
            self.emit_alias({
                'name': alias,
                'origin': data.get('origin'),
                'uid': data.get('uid'),
                'uid_canonical': data.get('uid_canonical'),
            })
        return data

    def get_entity(self, uid):
        data = self.entities.find_one(uid=uid)
        if data is not None:
            data['aliases'] = []
            for alias in self.aliases.find(uid=uid):
                data['aliases'].append(alias.get('name'))
        return data

    def delete_entity(self, uid):
        self.entities.delete(uid=uid)
        self.aliases.delete(uid=uid)
        self.links.delete(left_uid=uid)
        self.links.delete(right_uid=uid)

    def emit_alias(self, data):
        name = data.get('name') or ''
        name = name.strip()
        if not len(name):
            return
        data['name'] = name
        self.aliases.upsert(data, ['origin', 'uid', 'name'])

    def emit_link(self, data):
        if data['source'] is None or data['target'] is None:
            return
        if data['source'] == data['target']:
            return
        self.links.upsert(data, ['origin', 'source', 'target'])

    def emit_document(self, origin, url, title=None, uid=None,
                      query=None, publisher=None):
        url = urlnorm.norm(url)
        ref = get_uid(origin, url, uid or query)
        self.documents.upsert({
            'reference': ref,
            'origin': origin,
            'uid': uid,
            'query': query,
            'url': url,
            'publisher': publisher,
            'title': title,
        }, ['reference'])

    def emit_judgement(self, uida, uidb, judgement, score=None, decided=False):
        if uida is None or uidb is None:
            return
        if judgement is not None:
            decided = True
        data = {
            'left_uid': max(uida, uidb),
            'right_uid': min(uida, uidb),
            'judgement': judgement,
            'decided': decided
        }
        if score is not None:
            data['score'] = float(score)
        self.mappings.upsert(data, ['left_uid', 'right_uid'])

        if judgement is False:
            # Remove any entities predicated on this mapping.
            for entity in self.entities.find(match_uid=uida, query_uid=uidb):
                self.delete_entity(entity.get('uid'))
            for entity in self.entities.find(match_uid=uidb, query_uid=uida):
                self.delete_entity(entity.get('uid'))

    def get_judgement(self, uida, uidb):
        if uida is None or uidb is None:
            return
        data = self.mappings.find_one(left_uid=max(uida, uidb),
                                      right_uid=min(uida, uidb))
        if data is None:
            return None
        return data.get('judgement')

    def clear_mappings(self):
        self.mappings.delete(judgement=None, decided=False)

    def integrate(self):
        canonicalise(self)

    def iter_merged_entities(self):
        for entity in merge_entities(self):
            yield entity

    def iter_searches(self, min_weight=0):
        for entity in self.iter_merged_entities():
            if entity.get('tasked'):
                yield entity

    def iter_merged_links(self):
        for link in merge_links(self):
            yield link

    def enrich(self, name, origins=[], min_weight=0):
        enricher = get_enrichers().get(name)
        if enricher is None:
            raise RuntimeError("Enricher not found: %s" % name)
        sink = self.origin(name)
        for entity in self.iter_searches(min_weight=min_weight):
            if len(origins) and not entity.get('origin').intersection(origins):
                continue
            # pprint(entity)
            enricher(sink, entity)

    def flush(self):
        self.entities.drop()
        self.aliases.drop()
        self.links.drop()
