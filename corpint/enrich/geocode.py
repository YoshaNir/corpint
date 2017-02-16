
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

def geocode_address(origin, address):
    address = tidy_address(address)
    if address is None:
        return
    data = {}
    results = gmaps.geocode(address)
    if results:
        result =  results[0] 
        data['normalized_address'] = result['formatted_address']
        data['lat'] = result['geometry']['location']['lat']
        data['lng'] = result['geometry']['location']['lng']
        data['geocode_type'] = result['geometry']['location_type']
        # origin.log.info("Geocoder found results: %s" % address)
    else:
        origin.log.info("Geocoder found no results: %s" % address)
    return data

def emit_entity(origin, entity, data):
    if entity.get('dataset') is None:
        return
    entity_uid = origin.uid(entity.get('id'))
    if entity_uid is None:
        return
    data.update(map_properties(entity, ENTITY_PROPERTIES))
    origin.log.info("[%(dataset)s]: %(name)s", entity)
    origin.emit_entity(data)

def enrich(origin, entity):
    if not 'address' in entity or not entity['address']:
        return    
    address = list(entity['address'])[0]
    origin.log.info("Geocoding address: %s", address)
    data = geocode_address(origin, address)
    emit_entity(origin, entity, data)
