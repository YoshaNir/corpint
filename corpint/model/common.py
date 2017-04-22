from hashlib import sha1
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
UID_LENGTH = len(sha1().hexdigest())
