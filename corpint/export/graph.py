import fingerprints
from py2neo import Graph, Node, Relationship

from corpint.core import project, config
from corpint.model import Entity, Link, Mapping, Address, Document

ADDRESS = 'Address'
DOCUMENT = 'Document'
NAME = 'Name'


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
            node = Node(label, origin=entity.origin, **data)
            project.log.info("Node [%s]: %s", label, entity.name)
            tx.create(node)
            for uid in entity.uids:
                entities[uid] = node

            for name in entity.names:
                fp = fingerprints.generate(name)
                name_node = Node(NAME, name=name, fp=fp)
                tx.merge(name_node, NAME, 'fp')

                rel = Relationship(node, 'ALIAS', name_node)
                tx.create(rel)

        clear_leaf_nodes(tx, NAME)
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


def load_mappings(graph, entities, decided):
    """Load mappings which are decided but unsure, or undecided."""
    tx = graph.begin()
    q = Mapping.find_by_decision(decided)
    if decided:
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


def load_addresses(graph, entities):
    """Load addresses, geocoded or otherwise."""
    tx = graph.begin()
    project.log.info("Loading %s addresses...", Address.find().count())
    addresses = {}
    try:
        for address in Address.find():
            entity = entities.get(address.entity_uid)
            if entity is None:
                continue
            slug = address.display_slug
            if slug is None:
                continue
            if slug not in addresses:
                node = Node(ADDRESS, name=address.display_label, slug=slug)
                tx.create(node)
                addresses[slug] = node
            rel = Relationship(entity, 'LOCATED_AT', addresses[slug])
            tx.create(rel)
        clear_leaf_nodes(tx, ADDRESS)
        tx.commit()
    except Exception:
        tx.rollback()
        raise


def load_documents(graph, entities):
    """Load documents that mention multiple entities."""
    tx = graph.begin()
    project.log.info("Loading %s documents...", Document.find().count())
    documents = {}
    try:
        for document in Document.find():
            entity = entities.get(document.entity_uid)
            if entity is None:
                continue
            if document.uid not in documents:
                node = Node(DOCUMENT,
                            name=document.title,
                            url=document.url,
                            uid=document.uid)
                tx.create(node)
                documents[document.uid] = node
            rel = Relationship(entity, 'MENTIONS', documents[document.uid])
            tx.create(rel)
        clear_leaf_nodes(tx, DOCUMENT)
        tx.commit()
    except Exception:
        tx.rollback()
        raise


def export_to_neo4j(decided):
    if config.neo4j_uri is None:
        project.log.error("No $NEO4J_URI set, cannot load graph.")
        return

    project.log.info("Loading graph to Neo4J: %s", config.neo4j_uri)
    graph = Graph(config.neo4j_uri)
    graph.run('MATCH (n) DETACH DELETE n')

    Mapping.canonicalize()
    entities = load_entities(graph)
    load_links(graph, entities)
    load_mappings(graph, entities, decided)
    load_addresses(graph, entities)
    load_documents(graph, entities)
