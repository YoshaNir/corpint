from os import environ

PROJECT = environ.get('CORPINT_PROJECT', 'default')
DATABASE_URI = environ.get('DATABASE_URI')
NEO4J_URI = environ.get('NEO4J_URI')
