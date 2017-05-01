import logging
import requests
from time import sleep
from os import environ
from normality import latinize_text
from pprint import pprint  # noqa
from urlparse import urljoin
from itertools import count

from corpint.core import session
from corpint.model.schema import COMPANY, ORGANIZATION, PERSON, ASSET, OTHER
from corpint.model import Document

log = logging.getLogger(__name__)

API_KEY = environ.get('ALEPH_APIKEY')
HOST = environ.get('ALEPH_HOST', 'https://data.occrp.org')
ENTITIES_API = urljoin(HOST, 'api/1/entities')
DOCUMENTS_API = urljoin(HOST, 'api/1/query')
COLLECTIONS_API = urljoin(HOST, 'api/1/collections')
COLLECTIONS = {}
DATASETS_API = urljoin(HOST, 'api/1/datasets')
DATASETS = {}
STOPWORDS = ['mr ', 'mr. ', 'ms ', 'ms. ', 'mrs ', 'mrs. ', 'the ', 'a ']

ENTITY_PROPERTIES = {
    'summary': 'summary',
    'status': 'status',
    'sourceUrl': 'source_url',
    'legalForm': 'legal_form',
    'registrationNumber': 'registration_number',
    'country': 'country',
    'mainCountry': 'country',
    'jurisdiction': 'country',
    'nationality': 'country',
    'address': 'address',
    'birthDate': 'dob',
    'incorporationDate': 'incorporation_date',
    'dissolutionDate': 'dissolution_date',
}

LINK_PROPERTIES = {
    'role': 'summary',
    'summary': 'summary',
    'startDate': 'start_date',
    'endDate': 'end_date',
}

TYPE_MAPPING = {
    'Company': COMPANY,
    'Land': ASSET,
    'Person': PERSON,
    'LegalEntity': OTHER,
    'Organization': ORGANIZATION,
    'Concession': ASSET,
    'PublicBody': ORGANIZATION
}


def search_term(term):
    if term is None:
        return
    term = latinize_text(term)
    if term is None:
        return
    term = term.replace('"', ' ').strip().lower()
    for stopword in STOPWORDS:
        if term.startswith(stopword):
            term = term[len(stopword):]
    if len(term) < 4:
        return
    return term


def search_entity(entity):
    names = set()
    for name in entity.names:
        name = search_term(name)
        if name is not None:
            names.add(name)

    terms = set(names)
    for name in names:
        for other in names:
            if other != name and other in name and name in terms:
                terms.remove(name)

    searches = []
    for term in terms:
        search = '"%s"' % term
        if entity.schema == PERSON:
            search = search + '~2'
        searches.append(search)
    return ' OR '.join(searches)


def collection_label(id):
    if id not in COLLECTIONS:
        url = '%s/%s' % (COLLECTIONS_API, id)
        res = aleph_api(url)
        if res is None:
            return id
        COLLECTIONS[id] = res.get('label')
    return COLLECTIONS[id]


def dataset_label(id):
    if id not in DATASETS:
        url = '%s/%s' % (DATASETS_API, id)
        res = aleph_api(url)
        if res is None:
            return id
        DATASETS[id] = res.get('label')
    return DATASETS[id]


def aleph_api(url, params=None):
    params = params or dict()
    if API_KEY is not None:
        params['api_key'] = API_KEY

    for i in count(2):
        try:
            res = requests.get(url, params=params, verify=False)
            return res.json()
        except Exception as ex:
            log.exception(ex)
            sleep(i ** 2)


def aleph_paged(url, params=None, limit=None):
    params = params or dict()
    params['limit'] = 50
    params['offset'] = 0
    while True:
        data = aleph_api(url, params=params)
        if data is None:
            break
        # pprint(data)
        for result in data.get('results', []):
            yield result
        next_offset = params['offset'] + params['limit']
        total = limit or data.get('total', 0)
        if next_offset > total:
            break
        params['offset'] = next_offset


def map_properties(obj, mapping):
    data = {'aliases': set()}
    for key, values in obj.get('properties', {}).items():
        if key in ['alias', 'previousName']:
            data['aliases'].update(values)
        prop = mapping.get(key)
        if prop is not None:
            for value in values:
                data[prop] = value
    return data


def emit_entity(emitter, entity, links=True):
    # Skip collection stuff for now.
    if entity.get('dataset') is None:
        return

    entity_uid = emitter.uid(entity.get('id'))
    if entity_uid is None:
        return
    data = {
        'aleph_id': '%s:%s' % (entity.get('dataset'), entity.get('id')),
        'uid': entity_uid,
        'publisher': dataset_label(entity.get('dataset')),
        'name': entity.get('name')
    }
    data.update(map_properties(entity, ENTITY_PROPERTIES))
    data['type'] = TYPE_MAPPING.get(entity.get('schema'))
    emitter.log.info("[%(dataset)s]: %(name)s", entity)

    if links:
        links_url = '%s/%s/links' % (ENTITIES_API, entity.get('id'))
        for link in aleph_paged(links_url):
            remote = link.get('remote')
            links = data.get('type') == ASSET
            other_uid = emit_entity(emitter, remote, links=links)
            if other_uid is None:
                continue
            ldata = {
                'source_uid': other_uid if link['inverted'] else entity_uid,
                'target_uid': entity_uid if link['inverted'] else other_uid,
            }
            ldata.update(map_properties(link, LINK_PROPERTIES))
            ldata.pop('aliases')
            emitter.emit_link(ldata)

    emitter.emit_entity(data)
    return entity_uid


# def get_entity(origin, entity_id):
#     entity_uid = origin.uid(entity_id)
#     if origin.entity_exists(entity_uid):
#         return entity_uid
#     url = ENTITIES_API + '/%s' % entity_id
#     data = aleph_api(url)
#     return emit_entity(origin, data)


def enrich(origin, entity):
    if entity.schema not in [PERSON, OTHER, ORGANIZATION, COMPANY]:
        return

    names = set()
    for name in entity.names:
        term = search_term(name)
        if term is None:
            continue
        if entity.schema == PERSON:
            term = term + '~2'
        names.add(term)

    query = ' OR '.join(names)
    for match in aleph_paged(ENTITIES_API, params={'q': query}):
        match_uid = origin.uid(match.get('id'))
        if match_uid is None:
            continue
        emitter = origin.result(entity.uid, match_uid)
        emit_entity(emitter, match)


def search_documents(query):
    if query is None or not len(query.strip()):
        return
    for doc in aleph_paged(DOCUMENTS_API, params={'q': query}, limit=5000):
        url = urljoin(HOST, '/text/%s' % doc['id'])
        if doc.get('type') == 'tabular':
            url = urljoin(HOST, '/tabular/%s/0' % doc['id'])
        publisher = collection_label(doc.get('collection_id'))
        yield url, doc.get('title'), publisher


def enrich_documents(origin, entity):
    for uid in entity.uids:
        Document.delete_by_entity(entity.uid)
    session.commit()

    if entity.schema not in [PERSON, COMPANY, ORGANIZATION, OTHER]:
        return

    total = 0
    query = search_entity(entity)
    for url, title, publisher in search_documents(query):
        origin.emit_document(entity.uid, url, title, publisher=publisher)
        total += 1
    origin.log.info('Query [%s]: %s -> %s', entity.name, query, total)
