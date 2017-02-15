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


def aleph_paged(url, params=None):
    params = params or dict()
    params['limit'] = 50
    params['offset'] = 0
    while True:
        data = aleph_api(url, params=params)
        # pprint(data)
        for result in data.get('results', []):
            yield result
        next_offset = params['offset'] + params['limit']
        if next_offset > data.get('total', 0):
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
        'name': entity.get('name')
    }
    data['type'] = TYPE_MAPPING.get(entity.get('schema'))

    data.update(map_properties(entity, ENTITY_PROPERTIES))
    origin.log.info("[%(dataset)s]: %(name)s", entity)
    origin.emit_entity(data)

    if links:
        for link in aleph_paged(entity.get('api_url') + '/links'):
            other_uid = emit_entity(origin, link.get('remote'), links=False)
            if other_uid is None:
                continue
            ldata = {
                'source': other_uid if link['inverted'] else entity_uid,
                'target': entity_uid if link['inverted'] else other_uid,
            }
            ldata.update(map_properties(link, LINK_PROPERTIES))
            ldata.pop('aliases')
            origin.emit_link(ldata)

    return entity_uid


def enrich(origin, entity):
    for entity in aleph_paged(ENTITIES_API, params={'q': entity['name']}):
        emit_entity(origin, entity)



STOPWORDS = ['mr ', 'mr. ', 'ms ', 'ms. ',
             'mrs ', 'mrs. ', 'the ', 'a ']

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
    if not len(term):
        return
    return '"%s"' % term


def search_documents(query):
    for doc in aleph_paged(DOCUMENTS_API, params={'q': query}):
        url = urljoin(HOST, '/text/%s' % doc['id'])
        if doc.get('type') == 'tabular':
            url = urljoin(HOST, '/tabular/%s/0' % doc['id'])
        yield url, doc.get('title')


def enrich_documents(origin, entity):
    names = [search_term(n) for n in entity.get('names')]
    if entity['type'] == PERSON:
        names = [n + '~2' for n in names]
    names = ' OR '.join(set([n for n in names if n is not None]))
    total = 0
    for url, title in search_documents(search_term(names)):
        for uid in entity['uid_parts']:
            origin.emit_document(url, title, uid=uid, query=names)
        total += 1
    origin.log.info('Query [%s]: %s', total, names)

    for address in entity['address']:
        total = 0
        term = search_term(address) + '~3'
        for url, title in search_documents(term):
            origin.emit_document(url, title, query=address)
            total += 1
        origin.log.info('Query [%s]: %s', total, address)
