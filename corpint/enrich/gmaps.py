from os import environ
import googlemaps

from corpint.core import session
from corpint.model import Address

API_KEY = environ.get('GMAPS_APIKEY')


def remove_first_section_of_address(address):
    '''
    Often when an address fails to geocode, it's because it starts
    with a name. We could remove this with NLTK, but a lighter-weight
    way to do this is just to remove the element before the first comma.
    This will also give a rough location for street addresses that
    Google Maps doesn't know about.
    '''
    a = address.split(', ')
    if len(a) > 1:
        address = ', '.join(a[1:])
    return address


def geocode(gmaps, address):
    results = gmaps.geocode(address)
    if not len(results):
        d = remove_first_section_of_address(address)
        results = gmaps.geocode(d)
    return results


def enrich(origin, entity):
    gmaps = googlemaps.Client(key=API_KEY)
    for uid in entity.uids:
        q = Address.find_by_entity(uid)
        q = q.filter(Address.normalized == None)  # noqa
        for address in q:
            origin.log.info("Geocoding [%s] %s", entity.name, address.clean)
            results = geocode(gmaps, address.clean)
            if not len(results):
                origin.log.info("No results: %s" % address.clean)
            for result in results:
                address.update(normalized=result['formatted_address'],
                               latitude=result['geometry']['location']['lat'],
                               longitude=result['geometry']['location']['lng'])
                break
        session.commit()
