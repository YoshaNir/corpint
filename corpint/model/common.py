from hashlib import sha1
from normality import stringify
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
UID_LENGTH = len(sha1().hexdigest())


def is_list(obj):
    return isinstance(obj, (list, tuple, set))


def ensure_list(obj):
    """Make the returned object a list, otherwise wrap as single item."""
    if obj is None:
        return []
    if not is_list(obj):
        return [obj]
    return obj


class SchemaObject(object):
    MULTI = ['aliases']

    def parse_data(self, data):
        parsed = {f: [] for f in self.MULTI}
        for field, value in data.items():
            if field in self.MULTI:
                for value in ensure_list(value):
                    value = stringify(value)
                    if value is None or value in parsed[field]:
                        continue
                    parsed[field].append(value)
            else:
                value = stringify(value)
                if value is not None:
                    parsed[field] = value
        return parsed
