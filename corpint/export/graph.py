from py2neo import Graph, Node, Relationship

from corpint.core import project, config
from corpint.model import Entity, Link, Mapping


def clear_leaf_nodes(graph, label):
    graph.run("""MATCH ()-[r]->(n:%s)
        WITH n, collect(r) as rr
        WHERE length(rr) <= 1 AND NOT n-->()
        FOREACH (r IN rr | DELETE r)
        DELETE n
    """ % label)


def load_entities(graph):
    """Load composite entities into the graph."""
    tx = graph.begin()
    entities = {}
    try:
        for entity in Entity.iter_composite():
            label = entity.schema or 'Other'
            data = dict(entity.data)
            data.pop('aliases', None)
            node = Node(label, **data)
            project.log.info("Node [%s]: %s", label, entity.name)
            tx.create(node)
            for uid in entity.uids:
                entities[uid] = node
        tx.commit()
        return entities
    except Exception:
        tx.rollback()
        raise


def load_links(graph, entities):
    """Load explicit links into the graph."""
    tx = graph.begin()
    project.log.info("Loading %s links...", Link.find().count())
    try:
        for link in Link.find():
            source = entities.get(link.source_canonical_uid)
            target = entities.get(link.target_canonical_uid)
            if source is None or target is None:
                continue
            label = link.schema or 'LINK'
            rel = Relationship(source, label, target, **link.data)
            tx.create(rel)
        tx.commit()
    except Exception:
        tx.rollback()
        raise


def load_mappings(graph, entities):
    """Load mappings which are decided but unsure."""
    tx = graph.begin()
    q = Mapping.find_decided()
    q = q.filter(Mapping.judgement == None)  # noqa
    project.log.info("Loading %s mappings...", q.count())
    try:
        for mapping in q:
            left = entities.get(mapping.left_uid)
            right = entities.get(mapping.right_uid)
            if left is None or right is None:
                continue
            rel = Relationship(left, 'SIMILAR', right,
                               score=mapping.score)
            tx.create(rel)
        tx.commit()
    except Exception:
        tx.rollback()
        raise


def export_to_neo4j():
    if config.neo4j_uri is None:
        project.log.error("No $NEO4J_URI set, cannot load graph.")
        return

    project.log.info("Loading graph to Neo4J: %s", config.neo4j_uri)
    graph = Graph(config.neo4j_uri)
    graph.run('MATCH (n) DETACH DELETE n')

    Mapping.canonicalize()
    entities = load_entities(graph)
    load_links(graph, entities)
    load_mappings(graph, entities)

    # clear_leaf_nodes(graph, 'Name')
    # clear_leaf_nodes(graph, 'Address')
    # clear_leaf_nodes(graph, 'Document')
