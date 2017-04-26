from flask import Blueprint, request, url_for, redirect
from flask import render_template
from sqlalchemy import or_, func

from corpint.core import project, session
from corpint.model.mapping import Mapping, Entity

blueprint = Blueprint('base', __name__)

SKIP_FIELDS = ['name', 'aliases', 'source_url', 'opencorporates_url',
               'aleph_id']
JUDGEMENTS = {
    'TRUE': True,
    'FALSE': False,
    'NULL': None,
}


def common_fields_mapping(entity, mapping):
    other = mapping.get_other(entity)
    keys = set()
    for obj in [entity, other]:
        for k, v in obj.data.items():
            if v is not None and k not in SKIP_FIELDS:
                keys.add(k)
    return list(sorted([k for k in keys]))


def mapping_height(entity, mapping):
    return len(common_fields_mapping(entity, mapping)) + 2


def mapping_compare(entity, mapping):
    other = mapping.get_other(entity)
    for field in common_fields_mapping(entity, mapping):
        label = field.replace('_', ' ').capitalize()
        yield (label, entity.data.get(field), other.data.get(field))


def mapping_key(entity, mapping):
    other = mapping.get_other(entity)
    return 'judgement:%s:%s' % (entity.uid, other.uid)


def mapping_match(mapping, judgement, decisions):
    if mapping.decided:
        return mapping.judgement == judgement
    pair = Mapping.sort_uids(mapping.left_uid, mapping.right_uid)
    return judgement is decisions.get(pair, False)


@blueprint.app_context_processor
def template_context():
    return {
        'project': project.name.upper(),
        'mapping_compare': mapping_compare,
        'mapping_height': mapping_height,
        'mapping_key': mapping_key,
        'mapping_match': mapping_match,
    }


@blueprint.route('/', methods=['GET'])
def index():
    return redirect(url_for('.entities'))


@blueprint.route('/entities', methods=['GET'])
def entities():
    text_query = request.args.get('q', '').strip()
    offset = int(request.args.get('offset', '0'))
    limit = 50
    sq = session.query(Mapping.left_uid)
    sq = sq
    q = session.query(Entity)
    q = q.filter(Entity.project == project.name)
    q = q.filter(Entity.active == True)  # noqa
    if len(text_query):
        q = q.filter(Entity.data['name'].astext.ilike('%' + text_query + '%'))
    total = q.count()
    context = {
        'total': total,
        'has_prev': offset > 0,
        'has_next': total >= (offset + limit),
        'next': offset + limit,
        'prev': max(0, offset - limit),
        'text_query': text_query,
    }
    q = q.offset(offset).limit(limit)
    return render_template('entities.html', entities=q, **context)


@blueprint.route('/entity/<uid>', methods=['GET'])
def entity(uid):
    entity = Entity.get(uid)
    q = session.query(Mapping)
    q = q.filter(Mapping.project == project.name)
    q = q.filter(or_(
        Mapping.left_uid == entity.uid,
        Mapping.right_uid == entity.uid
    ))
    q = q.order_by(Mapping.score.desc())
    decisions = Mapping.get_decisions()
    undecided = q.filter(Mapping.decided == False)  # noqa
    decided = q.filter(Mapping.decided == True)  # noqa
    sections = (
        ('Undecided', undecided),
        ('Decided', decided)
    )
    return render_template('entity.html', entity=entity,
                           sections=sections, decisions=decisions)


@blueprint.route('/review', methods=['GET'])
def review_get(offset=None):
    """Retrieve two lists of possible equivalences to map."""
    limit = int(request.args.get('limit') or 3)
    offset = int(request.args.get('offset') or 0)
    candidates = Mapping.find_undecided(limit=limit, offset=offset)
    decisions = Mapping.get_decisions()
    return render_template('review.html', candidates=candidates,
                           decisions=decisions)


@blueprint.route('/review/entity', methods=['GET'])
def review_entity_get(offset=None):
    """Jump to the next entity that needs disambiguation."""
    qa = session.query(Mapping.left_uid.label('uid'),
                       func.sum(Mapping.score).label('num'))
    qa = qa.filter(Mapping.project == project.name)
    qa = qa.filter(Mapping.decided == False)  # noqa
    qa = qa.group_by(Mapping.left_uid)
    qb = session.query(Mapping.right_uid.label('uid'),
                       func.sum(Mapping.score).label('num'))
    qb = qb.filter(Mapping.project == project.name)
    qb = qb.filter(Mapping.decided == False)  # noqa
    qb = qa.group_by(Mapping.right_uid)
    sq = qa.union(qb).subquery()
    q = session.query(sq.c.uid, func.sum(sq.c.num))
    q = q.join(Entity, Entity.uid == sq.c.uid)
    q = q.filter(Entity.active == True)  # noqa
    q = q.group_by(sq.c.uid, Entity.tasked)
    q = q.order_by(Entity.tasked.desc())
    q = q.order_by(func.sum(sq.c.num).desc())
    q = q.order_by(func.random())
    if q.count() == 0:
        return redirect(url_for('.entities'))
    q = q.limit(1)
    return redirect(url_for('.entity', uid=q.scalar()))


@blueprint.route('/review', methods=['POST'])
def review_post():
    """Retrieve two lists of possible equivalences to map."""
    offset = int(request.args.get('offset') or 0)
    for key, value in request.form.items():
        if not key.startswith('judgement:'):
            continue
        _, left, right = key.split(':', 2)
        value = JUDGEMENTS.get(value)
        project.emit_judgement(left, right, value, decided=True)
    action = request.form.get('action')
    if action:
        if action == 'next':
            return redirect(url_for('.review_entity_get'))
        return redirect(url_for('.entity', uid=action))
    return redirect(url_for('.review_get', offset=offset))
