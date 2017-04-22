import click
from pprint import pprint  # noqa
from dalet import parse_boolean
from unicodecsv import DictReader, DictWriter

# from corpint import project as make_project
from corpint.core import config, project
from corpint.model import Mapping, Entity
from corpint.webui import run_webui
from corpint.export import export_to_neo4j


@click.group()
@click.option('--debug/--no-debug', default=False)
@click.option('database_uri', '--db', envvar='DATABASE_URI')
@click.option('name', '--project', envvar='CORPINT_PROJECT')
def cli(debug, database_uri, name):
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


@cli.command()
def webui():
    run_webui()


@cli.command()
@click.option('origins', '--origin', '-o', multiple=True)
@click.argument('enricher')
def enrich(origins, enricher):
    project.enrich(enricher, origins=origins)


# @cli.command()
# @click.pass_context
# @click.option('origins', '--origin', '-o', multiple=True)
# def namemerge(ctx, origins):
#     name_merge(ctx.obj['PROJECT'], origins)


@cli.command()
@click.argument('origin')
def clear(origin):
    """Delete all the data from an origin."""
    origin = ctx.obj['PROJECT'].origin(origin)
    origin.clear()


@cli.command()
def searches():
    for entity in ctx.obj['PROJECT'].iter_searches():
        pprint(entity)


@cli.command()
@click.option('neo4j_uri', '--url', '-u')
def export_neo4j(neo4j_uri):
    export_to_neo4j(neo4j_uri)



def main():
    cli(obj={})


if __name__ == '__main__':
    main()
