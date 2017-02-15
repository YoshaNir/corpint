#!/usr/bin/env python
from flask import Blueprint, request, url_for, redirect
from flask import render_template, current_app

from corpint.integrate import train_judgement, get_decided, sorttuple
from corpint.integrate.dupe import to_record

blueprint = Blueprint('base', __name__)

SKIP_FIELDS = ['name', 'origin', 'fingerprint', 'uid']
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


@blueprint.route('/undecided', methods=['GET'])
def undecided_get():
    """Retrieve two lists of possible equivalences to map."""
    pairs = current_app.deduper.uncertainPairs()
    candidates = []
    for (left, right) in pairs:
        candidate = {'left': left, 'right': right}
        candidate['fields'] = common_fields(left, right)
        candidate['height'] = len(candidate['fields']) + 2
        candidates.append(candidate)
    return render_template('undecided.html',
                           candidates=candidates)


@blueprint.route('/undecided', methods=['POST'])
def undecided_post():
    """Retrieve two lists of possible equivalences to map."""
    judgement = JUDGEMENTS.get(request.form.get('judgement'))
    left = request.form.get('left')
    right = request.form.get('right')
    current_app.project.emit_judgement(left, right, judgement, trained=True)
    train_judgement(current_app.project, current_app.deduper,
                    left, right, judgement)
    return undecided_get()


@blueprint.route('/scored', methods=['GET'])
def scored_get(offset=None):
    """Retrieve two lists of possible equivalences to map."""
    project = current_app.project
    decided = get_decided(project)
    project.log.info("Doing extra checks with %s decisions", len(decided))
    offset = offset or int(request.args.get('offset') or 0)
    args = {
        'table': project.mappings.table.name,
        'limit': int(request.args.get('limit') or 25),
        'offset': offset,
    }
    query = """
        SELECT left_uid, right_uid, score
        FROM %(table)s WHERE judgement IS NULL
        ORDER BY score DESC
        LIMIT %(limit)s
        OFFSET %(offset)s
    """ % args
    while True:
        try_again = False
        candidates = []
        for data in project.db.query(query):
            left_uid, right_uid = data['left_uid'], data['right_uid']
            if sorttuple(left_uid, right_uid) in decided:
                project.mappings.delete(left_uid=left_uid,
                                        right_uid=right_uid)
                project.log.info("Deleted redudant mapping challenge.")
                try_again = True
                continue
            left = project.entities.find_one(uid=left_uid)
            right = project.entities.find_one(uid=right_uid)
            if left is None or right is None:
                project.mappings.delete(left_uid=left_uid,
                                        right_uid=right_uid)
                try_again = True
                continue
            left, right = to_record(left), to_record(right)
            candidates.append({
                'left': left,
                'right': right,
                'score': data['score'],
                'key': 'judgement:%s:%s' % (left_uid, right_uid),
                'fields': common_fields(left, right),
                'height':  len(common_fields(left, right)) + 2
            })
        if try_again:
            continue
        return render_template('scored.html', candidates=candidates)


@blueprint.route('/scored', methods=['POST'])
def scored_post():
    """Retrieve two lists of possible equivalences to map."""
    offset = int(request.args.get('offset') or 0)
    emit_judgement = current_app.project.emit_judgement
    for key, value in request.form.items():
        if not key.startswith('judgement:'):
            continue
        _, left, right = key.split(':', 2)
        judgement = JUDGEMENTS.get(value)
        if judgement is None:
            offset += 1
        emit_judgement(left, right, judgement, trained=False)
    return redirect(url_for('.scored_get', offset=offset))
