import logging
import dataset
import urlnorm
import countrynames
from pprint import pprint  # noqa
from normality import stringify
from sqlalchemy import Boolean, Unicode, Float

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
        ensure_column(self.mappings, 'score', Float)
        ensure_column(self.mappings, 'judgement_attribution', Unicode)
        ensure_column(self.mappings, 'left_uid', Unicode)
        ensure_column(self.mappings, 'right_uid', Unicode)

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

        try:
            data['weight'] = int(data.get('weight', 0))
        except Exception:
            raise ValueError("Invalid weight: %r", data)

        if 'country' in data:
            data['country'] = countrynames.to_code(data['country'])

        name = data.get('name')
        name = stringify(name)
        data['name'] = name

        for k, v in data.items():
            if k == 'aliases':
                continue
            data[k] = stringify(v)
            if data[k] is None:
                data.pop(k)

        # TODO: partial dates
        aliases = data.pop('aliases', [])
        self.entities.upsert(data, ['origin', 'uid'])
        for alias in aliases:
            self.emit_alias({
                'name': alias,
                'origin': data.get('origin'),
                'uid': data.get('uid'),
                'uid_canonical': data.get('uid_canonical'),
            })
        return data

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

    def emit_judgement(self, uida, uidb, judgement,
                       trained=False, score=None):
        if uida is None or uidb is None:
            return
        data = {
            'left_uid': max(uida, uidb),
            'right_uid': min(uida, uidb),
            'judgement': judgement,
            'trained': trained
        }
        if score is not None:
            data['score'] = float(score)
        self.mappings.upsert(data, ['left_uid', 'right_uid'])

    def integrate(self):
        canonicalise(self)

    def iter_merged_entities(self):
        for entity in merge_entities(self):
            yield entity

    def iter_searches(self, min_weight=0):
        for entity in self.iter_merged_entities():
            if entity['weight'] >= min_weight:
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
