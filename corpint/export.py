import fingerprints
from py2neo import Graph, Node, Relationship

from corpint import env


def normalise(data):
    properties = {}
    for key, value in data.items():
        if isinstance(value, (set, tuple)):
            value = list(value)
        if value is None:
            continue
        properties[key] = value
    return properties


def clear_leaf_nodes(graph, label):
    graph.run("""MATCH ()-[r]->(n:%s)
        WITH n, collect(r) as rr
        WHERE length(rr) <= 1 AND NOT n-->()
        FOREACH (r IN rr | DELETE r)
        DELETE n
    """ % label)


def load_entity(project, graph, entity):
    tx = graph.begin()
    try:
        label = entity.pop('type', None) or 'Other'
        node = Node(label, **normalise(entity))
        project.log.info("Node [%s]: %s", label, entity['name'])
        tx.create(node)

        # create "Name" fake nodes
        fps = set()
        for name in entity.get('names', []):
            fp = fingerprints.generate(name)
            if fp is None:
                continue
            fp = fp.replace(' ', '-')
            if fp in fps:
                continue
            fps.add(fp)
            alias = Node('Name', name=name, fp=fp)
            tx.merge(alias, 'Name', 'fp')
            rel = Relationship(node, 'ALIAS', alias)
            tx.create(rel)

        addrfps = set()
        for address in entity.get('address'):
            fp = fingerprints.generate(address)
            if fp is None:
                continue
            fp = fp.replace(' ', '-')
            if fp in addrfps:
                continue
            addrfps.add(fp)
            loc = Node('Address', name=address, fp=fp)
            tx.merge(loc, 'Address', 'fp')
            rel = Relationship(node, 'LOCATION', loc)
            tx.create(rel)

        for doc in project.documents.find(uid=entity['uid']):
            title = doc.get('title') or doc.get('url')
            project.log.info(" -> Node [Document]: %s", title)
            doc = Node('Document', name=title,
                       link=doc.get('url'),
                       publisher=doc.get('publisher'),
                       hash=doc.get('hash'))
            tx.merge(doc, 'Document', 'hash')
            rel = Relationship(node, 'MENTIONS', doc)
            tx.create(rel)

        tx.commit()
        return entity['uid'], node
    except Exception as err:
        tx.rollback()
        return None, None


def load_to_neo4j(project, neo4j_uri=None):
    neo4j_uri = neo4j_uri or env.NEO4J_URI
    if neo4j_uri is None:
        project.log.error("No $NEO4J_URI set, cannot load graph.")
        return
    project.log.info("Loading graph to Neo4J: %s", neo4j_uri)
    graph = Graph(neo4j_uri)

    graph.run('MATCH (n) DETACH DELETE n')
    entities = {}
    for entity in project.iter_merged_entities():
        uid, node = load_entity(project, graph, entity)
        entities[uid] = node

    for link in project.iter_merged_links():
        source = entities.get(link.pop('source'))
        target = entities.get(link.pop('target'))
        if source is None or target is None or source == target:
            continue
        rel = Relationship(source, 'LINK', target, **normalise(link))
        graph.create(rel)

    clear_leaf_nodes(graph, 'Name')
    # clear_leaf_nodes(graph, 'Address')
    # clear_leaf_nodes(graph, 'Document')
