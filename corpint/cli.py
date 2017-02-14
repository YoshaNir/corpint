import click

from corpint import project as make_project
from corpint import env
from corpint.webui import run_webui
from corpint.integrate import name_merge


@click.group()
@click.option('--debug/--no-debug', default=False)
@click.option('--db', envvar='DATABASE_URI')
@click.option('prefix', '--project', envvar='CORPINT_PROJECT')
@click.pass_context
def cli(ctx, debug, db, prefix):
    env.DEBUG = env.DEBUG or debug
    ctx.obj['PROJECT'] = make_project(prefix, db)


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
@click.option('simple', '--simple', default=False, is_flag=True)
def candidates(ctx, threshold, simple):
    from corpint.integrate import generate_candidates
    if simple:
        from corpint.integrate import generate_candidates_simple as generate_candidates
    generate_candidates(ctx.obj['PROJECT'], threshold=threshold)


@cli.command()
@click.pass_context
@click.option('origins', '--origin', '-o', multiple=True)
@click.option('minweight', '--min-weight', '-w', type=int, default=0)
@click.argument('enricher')
def enrich(ctx, origins, minweight, enricher):
    ctx.obj['PROJECT'].enrich(enricher, origins=origins, min_weight=minweight)


@cli.command()
@click.pass_context
@click.option('origins', '--origin', '-o', multiple=True)
def namemerge(ctx, origins):
    name_merge(ctx.obj['PROJECT'], origins)


def main():
    cli(obj={})


if __name__ == '__main__':
    main()
