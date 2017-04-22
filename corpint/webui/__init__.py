from os import path
from flask import Flask

from corpint.core import config
from corpint.webui.views import blueprint


def run_webui():
    dir_name = path.dirname(__file__)
    app = Flask('corpint',
                static_folder=path.join(dir_name, 'static'),
                template_folder=path.join(dir_name, 'templates'))
    app.register_blueprint(blueprint)
    app.debug = config.debug
    app.run(host='0.0.0.0')
