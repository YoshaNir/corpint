from flask import Blueprint, request, url_for, redirect
from flask import render_template

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


def common_fields(left, right):
    keys = set()
    for obj in [left, right]:
        for k, v in obj.items():
            if v is not None and k not in SKIP_FIELDS:
                keys.add(k)
    return list(sorted([k for k in keys]))


@blueprint.app_context_processor
def globals():
    return {'project': project.name.upper()}


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
    return render_template('entity.html', entity=entity)


@blueprint.route('/review', methods=['GET'])
def review_get(offset=None):
    """Retrieve two lists of possible equivalences to map."""
    limit = int(request.args.get('limit') or 3)
    offset = int(request.args.get('offset') or 0)
    candidates = []
    for mapping in Mapping.find_undecided(limit=limit, offset=offset):
        left = mapping.left
        right = mapping.right
        candidates.append({
            'left': left,
            'right': right,
            'score': mapping.score,
            'key': 'judgement:%s:%s' % (left.uid, right.uid),
            'fields': common_fields(left.data, right.data),
            'height':  len(common_fields(left.data, right.data)) + 2
        })
    return render_template('review.html', candidates=candidates,
                           section='review')


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
    return redirect(url_for('.review_get', offset=offset))
