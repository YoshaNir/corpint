import logging
from os import path
from pprint import pprint  # noqa
import corpint

log = logging.getLogger('test')
db_uri = 'sqlite:///test.sqlite3'
project = corpint.project('test', db_uri)

log.info("Creating sample entity...")
origin = project.origin('luke')
origin.emit_entity({
    'uid': origin.uid('calderbank'),
    'type': 'Person',
    'name': 'Damian Calderbank',
    'aliases': ['Damian James Calderbank', 'Damian J. Calderbank'],
    'weight': 5,
    'jurisdiction': 'United Kingdom'
})

log.info("Reading CSV file...")
with open(path.join(path.dirname(__file__), 'test.csv')) as fh:
    origin = project.origin(fh.name)
    origin.clear()
    for entity in corpint.load.csv(fh):
        entity['uid'] = origin.uid(entity['name'])
        entity['weight'] = 1
        origin.emit_entity(entity)

log.info("Data integration...")
# project.integrate(auto_match=True)
# project.enrich('wikipedia')
# project.integrate(auto_match=True)
# project.enrich('wikidata')
# project.enrich('bvdorbis')

log.info("Geocoding addresses...")
project.enrich('gmaps')

from corpint.export import load_to_neo4j
load_to_neo4j(project, 'http://neo4j:test@localhost:7474')
