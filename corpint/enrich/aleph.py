import logging
import requests
from time import sleep
from os import environ
from normality import latinize_text
from pprint import pprint  # noqa
from urlparse import urljoin
from itertools import count

from corpint.schema import COMPANY, ORGANIZATION, PERSON, ASSET, OTHER

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
    return '"%s"' % term


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


def emit_entity(origin, entity, links=True):
    # Skip collection stuff for now.
    if entity.get('dataset') is None:
        return

    entity_uid = origin.uid(entity.get('id'))
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
    origin.log.info("[%(dataset)s]: %(name)s", entity)

    if links:
        links_url = '%s/%s/links' % (ENTITIES_API, entity.get('id'))
        for link in aleph_paged(links_url):
            remote = link.get('remote')
            links = data.get('type') == ASSET
            other_uid = emit_entity(origin, remote, links=links)
            if other_uid is None:
                continue
            ldata = {
                'source': other_uid if link['inverted'] else entity_uid,
                'target': entity_uid if link['inverted'] else other_uid,
            }
            ldata.update(map_properties(link, LINK_PROPERTIES))
            ldata.pop('aliases')
            origin.emit_link(ldata)

    origin.emit_entity(data)
    return entity_uid


# def get_entity(origin, entity_id):
#     entity_uid = origin.uid(entity_id)
#     if origin.entity_exists(entity_uid):
#         return entity_uid
#     url = ENTITIES_API + '/%s' % entity_id
#     data = aleph_api(url)
#     return emit_entity(origin, data)


def enrich(origin, entity):
    if entity['type'] not in [PERSON, OTHER, ORGANIZATION, COMPANY]:
        return

    names = set()
    for name in entity.get('names'):
        term = search_term(name)
        if term is None:
            continue
        if entity['type'] == PERSON:
            term = term + '~2'
        names.add(term)

    query = ' OR '.join(names)
    for entity in aleph_paged(ENTITIES_API, params={'q': query}):
        emit_entity(origin, entity)


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
    for uid in entity['uid_parts']:
        origin.project.documents.delete(uid=uid)

    if entity['type'] != ASSET:
        names = set()
        for name in entity.get('names'):
            term = search_term(name)
            if term is None:
                continue
            if entity['type'] == PERSON:
                term = term + '~2'
            names.add(term)

        names = ' OR '.join(names)
        total = 0
        for url, title, publisher in search_documents(names):
            for uid in entity['uid_parts']:
                origin.emit_document(url, title, uid=uid, query=names,
                                     publisher=publisher)
            total += 1
        origin.log.info('Query [%s]: %s -> %s',
                        total, entity.get('name'), names)

    for address in entity['address']:
        total = 0
        origin.project.documents.delete(query=address)
        term = search_term(address)
        if term is None:
            continue
        term = term + '~3'
        for url, title, publisher in search_documents(term):
            origin.emit_document(url, title, query=address,
                                 publisher=publisher)
            total += 1
        origin.log.info('Query [%s]: %s', total, address)
