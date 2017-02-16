
from os import environ
import googlemaps
from normality import latinize_text

API_KEY = environ.get('GMAPS_APIKEY')
gmaps = googlemaps.Client(key=API_KEY)


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
    address = address.replace('<br/>', ', ')
    address = address.replace('\n', ', ')
    address = address.strip()
    return address


def enrich(origin, entity):
    for uid in entity['uid_parts']:
        entity = origin.project.entities.find_one(uid=uid)
        address = entity.get('address')
        if address is None or entity.get('normalized_address') is not None:
            continue
        origin.log.info("Geocoding: %s", address)
        address = tidy_address(address)
        if address is None:
            return
        results = gmaps.geocode(address)
        if not len(results):
            origin.log.info("Geocoder found no results: %s" % address)
            continue
        for result in gmaps.geocode(address):
            entity['address_canonical'] = result['formatted_address']
            entity['lat'] = result['geometry']['location']['lat']
            entity['lng'] = result['geometry']['location']['lng']
            entity['geocode_type'] = result['geometry']['location_type']
            # origin.log.info("Geocoder found results: %s" % address)
            origin.project.emit_entity(entity)
            break
        
