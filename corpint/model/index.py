from whoosh.filedb.filestore import RamStorage
from whoosh.qparser import QueryParser
from whoosh.query import Term
from whoosh.fields import Schema, TEXT, ID, KEYWORD

from corpint.core import project
from corpint.model.entity import Entity

schema = Schema(uid=ID(stored=True), fingerprint=TEXT,
                country=KEYWORD, name=TEXT(stored=True))


class EntityIndex(object):

    def __init__(self):
        storage = RamStorage()
        self.index = storage.create_index(schema)

    def build(self):
        project.log.info("Building entity search index...")
        writer = self.index.writer()
        q = Entity.find_by_origins(origins=[])
        q = q.filter(Entity.active == True)  # noqa
        count = 0
        for entity in q:
            for fp in entity.fingerprints:
                writer.add_document(uid=entity.uid, fingerprint=fp,
                                    country=entity.country, name=entity.name)
            count += 1
        writer.commit()
        project.log.info("Indexed %s entities.", count)

    def search_similar(self, entity, skip=[]):
        with self.index.searcher() as searcher:
            qp = QueryParser("fingerprint", schema=self.index.schema)
            # parser.add_plugin(qparser.FuzzyTermPlugin())
            tokens = set()
            for fp in entity.fingerprints:
                tokens.update(fp.split())
            if entity.country:
                tokens.add('country:%s' % entity.country)
            tokens = ' OR '.join(tokens)
            tokens = ['(%s)' % tokens]
            for uid in skip:
                tokens.append('(NOT uid:%s)' % uid)
            q = ' AND '.join(tokens)
            q = qp.parse(q)
            restrict_q = Term("uid", entity.uid)
            for result in searcher.search(q, mask=restrict_q):
                yield result.get('uid')
