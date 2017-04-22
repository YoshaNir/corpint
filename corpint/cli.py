import click
from pprint import pprint  # noqa

from corpint import project as make_project
from corpint.webui import run_webui
from corpint.export import export_to_neo4j
from corpint.integrate import name_merge, generate_candidates
from corpint.integrate import export_mappings as export_mappings_
from corpint.integrate import import_mappings as import_mappings_


@click.group()
@click.option('--debug/--no-debug', default=False)
@click.option('--db', envvar='DATABASE_URI')
@click.option('name', '--project', envvar='CORPINT_PROJECT')
@click.pass_context
def cli(ctx, debug, db, name):
    ctx.obj['PROJECT'] = make_project(name, db)


@cli.command()
@click.pass_context
def webui(ctx):
    run_webui(ctx.obj['PROJECT'])


@cli.command()
@click.pass_context
def integrate(ctx):
    ctx.obj['PROJECT'].integrate()


@cli.command()
@click.pass_context
@click.option('threshold', '--threshold', '-t', type=float, default=0.5)
@click.option('origins', '--origin', '-o', multiple=True)
def candidates(ctx, threshold, origins):
    generate_candidates(ctx.obj['PROJECT'], origins=origins,
                        threshold=threshold)


@cli.command()
@click.pass_context
@click.option('origins', '--origin', '-o', multiple=True)
@click.argument('enricher')
def enrich(ctx, origins, enricher):
    ctx.obj['PROJECT'].enrich(enricher, origins=origins)


@cli.command()
@click.pass_context
@click.option('origins', '--origin', '-o', multiple=True)
def namemerge(ctx, origins):
    name_merge(ctx.obj['PROJECT'], origins)


@cli.command()
@click.pass_context
@click.argument('origin')
def clear(ctx, origin):
    origin = ctx.obj['PROJECT'].origin(origin)
    origin.clear()


@cli.command()
@click.pass_context
def clear_mappings(ctx):
    ctx.obj['PROJECT'].clear_mappings()


@cli.command()
@click.pass_context
def searches(ctx):
    for entity in ctx.obj['PROJECT'].iter_searches():
        pprint(entity)


@cli.command()
@click.pass_context
@click.option('neo4j_uri', '--url', '-u')
def export_neo4j(ctx, neo4j_uri):
    export_to_neo4j(ctx.obj['PROJECT'], neo4j_uri)


@cli.command()
@click.pass_context
@click.argument('filename')
def export_mappings(ctx, filename):
    export_mappings_(ctx.obj['PROJECT'], filename)


@cli.command()
@click.pass_context
@click.argument('filename')
def import_mappings(ctx, filename):
    import_mappings_(ctx.obj['PROJECT'], filename)


def main():
    cli(obj={})


if __name__ == '__main__':
    main()
