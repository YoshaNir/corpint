import click
from pprint import pprint  # noqa
from dalet import parse_boolean
from collections import defaultdict
from itertools import combinations
from unicodecsv import DictReader, DictWriter

from corpint.core import config, project
from corpint.model import Mapping, Entity
from corpint.webui import run_webui
from corpint.export import export_to_neo4j
from corpint.enrich import get_enrichers


@click.group()
@click.option('--debug/--no-debug', default=False)
@click.option('database_uri', '--db', envvar='DATABASE_URI')
@click.option('name', '--project', envvar='CORPINT_PROJECT')
def cli(debug, database_uri, name):
    """An investigative graph data assembly toolkit."""
    config.debug = debug
    config.database_uri = database_uri
    config.project_name = name


@cli.group()
def mappings():
    """Manage record linkage mappings."""


@mappings.command('generate')
@click.option('threshold', '--threshold', '-t', type=float, default=0.5)
@click.option('origins', '--origin', '-o', multiple=True)
def mappings_generate(threshold, origins):
    """Compare all entities and generate candidates."""
    Mapping.generate_scored_mappings(origins=origins, threshold=threshold)


@mappings.command('apply')
def mappings_apply():
    """Apply mapped canonical IDs to all entities."""
    Mapping.canonicalize()


@mappings.command('cleanup')
def mappings_cleanup():
    """Delete undecided generated mappings."""
    Mapping.cleanup()


@mappings.command('export')
@click.argument('file', type=click.File('wb'))
def mappings_export(file):
    """Export decided mappings to a CSV file."""
    writer = DictWriter(file, fieldnames=['left', 'right', 'judgement'])
    writer.writeheader()
    for mapping in Mapping.find_decided():
        writer.writerow({
            'left': mapping.left_uid,
            'right': mapping.right_uid,
            'judgement': mapping.judgement
        })


@mappings.command('import')
@click.argument('file', type=click.File('rb'))
def mappings_import(file):
    """Load decided mappings from a CSV file."""
    for row in DictReader(file):
        left_uid = row.get('left')
        right_uid = row.get('right')
        judgement = parse_boolean(row.get('judgement'), default=None)
        score = None
        if judgement is None:
            left = Entity.get(left_uid)
            right = Entity.get(right_uid)
            score = left.compare(right)
        project.emit_judgement(left_uid, right_uid, judgement,
                               score=score, decided=True)


@mappings.command('crunch')
@click.pass_context
@click.option('origins', '--origin', '-o', multiple=True)
def mappings_crunch(ctx, origins):
    """Merge all entities with similar names (bad idea)."""
    inverted = defaultdict(set)
    for entity in Entity.find_by_origins(origins):
        for fp in entity.fingerprints:
            inverted[entity.uid] = fp

    for name, uids in inverted.items():
        if len(uids) == 1:
            continue

        project.log.info("Merge: %s (%d matches)", name, len(uids))
        for (left, right) in combinations(uids, 2):
            project.emit_judgement(left, right, True)


@cli.command()
def webui():
    """Record linkage web interface."""
    run_webui()


@cli.command()
@click.option('origins', '--origin', '-o', multiple=True)
@click.argument('enricher')
def enrich(origins, enricher):
    """Cross-reference against external APIs."""
    enrich_func = get_enrichers().get(enricher)
    if enrich_func is None:
        raise RuntimeError("Enricher not found: %s" % enricher)
    emitter = project.origin(enricher)
    Mapping.canonicalize()
    for entity in Entity.iter_composite(origins=origins, tasked=True):
        enrich_func(emitter, entity)


@cli.command()
@click.argument('origin')
def clear(origin):
    """Delete all the data from an origin."""
    project.origin(origin).clear()


@cli.group()
def export():
    """Export the generated graph elsewhere."""


@export.command('neo4j')
@click.option('neo4j_uri', '--url', '-u', default=None)
def export_neo4j(neo4j_uri):
    """Load the graph to Neo4J for navigation."""
    if neo4j_uri is not None:
        config.neo4j_uri = neo4j_uri
    export_to_neo4j()


def main():
    cli(obj={})


if __name__ == '__main__':
    main()
