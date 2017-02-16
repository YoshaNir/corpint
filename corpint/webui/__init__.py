from os import path
from flask import Flask

from corpint import env
from corpint.webui.views import blueprint


def run_webui(project):
    dir_name = path.dirname(__file__)
    app = Flask('corpint',
                static_folder=path.join(dir_name, 'static'),
                template_folder=path.join(dir_name, 'templates'))
    app.register_blueprint(blueprint)
    app.debug = env.DEBUG
    app.project = project
    app.run(host='0.0.0.0')
