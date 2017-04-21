from os import environ
import googlemaps
from normality import latinize_text

API_KEY = environ.get('GMAPS_APIKEY')


def tidy_address(address):
    address = address.upper()
    if 'ESQ,' in address or 'ESQ.,' in address:
        a = address.split('ESQ,')
        if len(a) == 1:
            a = address.split('ESQ.,')
        address = ', '.join(a[1:])
    address = address.strip()
    if address.startswith('ATTENTION') \
        or address.startswith('ATTN') \
        or address.startswith('C/O'):

        a = address.split(',')
        if len(a) > 1:
            address = ', '.join(a[1:])
    if address is None:
        return
    address = latinize_text(address)
    # Don't try to geocode country codes or NONE
    if address is None or len(address) < 3 or address == 'NONE':
        return
    address = address.replace('UNDELIVERABLE DOMESTIC ADDRESS', '')
    address = address.replace('<BR/>', ', ')
    address = address.replace('\n', ', ')
    address = address.strip()
    return address


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
    for uid in entity['uid_parts']:
        entity = origin.project.entities.find_one(uid=uid)
        address = entity.get('address')
        if address is None or entity.get('address_canonical') is not None:
            continue
        origin.log.info("Geocoding [%s] %s @ %s", entity.get('origin'),
                        entity.get('name'), address)
        address = tidy_address(address)
        if address is None:
            return
        results = geocode(gmaps, address)
        if not len(results):
            origin.log.info("Geocoder found no results: %s" % address)
            continue
        for result in gmaps.geocode(address):
            origin.project.entities.update({
                'address': address,
                'address_canonical': result['formatted_address'],
                'lat': result['geometry']['location']['lat'],
                'lng': result['geometry']['location']['lng']
            }, ['address'])
            break
