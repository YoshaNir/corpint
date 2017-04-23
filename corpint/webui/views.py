from flask import Blueprint, request, url_for, redirect
from flask import render_template, current_app

from corpint.core import project
from corpint.model.mapping import Mapping

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


@blueprint.route('/', methods=['GET'])
def index():
    return redirect(url_for('.scored_get'))


@blueprint.route('/scored', methods=['GET'])
def scored_get(offset=None):
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
    return render_template('scored.html', candidates=candidates)


@blueprint.route('/scored', methods=['POST'])
def scored_post():
    """Retrieve two lists of possible equivalences to map."""
    offset = int(request.args.get('offset') or 0)
    for key, value in request.form.items():
        if not key.startswith('judgement:'):
            continue
        _, left, right = key.split(':', 2)
        value = JUDGEMENTS.get(value)
        project.emit_judgement(left, right, value, decided=True)
    return redirect(url_for('.scored_get', offset=offset))
