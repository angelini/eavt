from enum import Enum
from functools import wraps


class AttributeNotFound(Exception):
    def __init__(self, entity, attribute):
        self.message = '{}.{} not found: {}.\{{}\}' \
            .format(entity, attribute, entity, ', '.join(entity.attributes))


class AttributeType(Enum):
    INT = 1
    STRING = 2
    DECIMAL = 3
    TIMESTAMP = 4


class Attribute:
    def __init__(self, entity, name, attr_type, inputs=None):
        self.entity = entity
        self.name = name
        self.attr_type = attr_type
        self.inputs = inputs or {}

    def __repr__(self):
        return 'Attribute(Entity({}), {}, {})' \
            .format(self.entity.name, self.name, self.attr_type)


class Entity:
    def __init__(self, name, attr_defs):
        self.name = name
        self.attributes = {
            name: Attribute(self, name, attr_type, inputs=inputs)
            for name, (inputs, attr_type) in attr_defs.items()
        }

    def __repr__(self):
        attr_names = [attr_name for attr_name in self.attributes.keys()]
        return 'Entity({}, {})'.format(self.name, attr_names)


class Source(Entity):
    def __init__(self, path):
        super().__init__(path, self._load_schema(path))

    def __getitem__(self, name):
        if name not in self.attributes:
            raise AttributeNotFound(self, name)
        return self.attributes[name]

    @staticmethod
    def _load_schema(path):
        return {
            name: ([], attr_type)
            for (name, attr_type) in SCHEMAS[path].items()
        }


class Derived(Entity):
    def __init__(self):
        name = self.__class__.__name__.lower()
        attributes = {}

        for attr_name in dir(self):
            class_attr = getattr(self, attr_name)
            if hasattr(class_attr, '_output'):
                attributes[attr_name] = (class_attr._inputs, class_attr._output)

        super().__init__(name, attributes)


class Join:
    def __init__(self, entity, name, match_name='id'):
        self.entity = entity
        self.name = name
        self.match_name = match_name

    def __repr__(self):
        return 'Join(Entity({}), {}, {})' \
            .format(self.entity.name, self.name, self.match_name)


def input(name, selector):
    def input_decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            return fn(*args, **kwargs)

        if not hasattr(wrapper, '_inputs'):
            wrapper._inputs = {}
        wrapper._inputs[name] = selector
        return wrapper

    return input_decorator


def output(attr_type):
    def output_decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            return fn(*args, **kwargs)

        wrapper._output = attr_type
        return wrapper

    return output_decorator

# --- DATA ---

SCHEMAS = {
    'data/raw/orders': {
        'id': AttributeType.INT,
        'shop_id': AttributeType.INT,
        'customer_id': AttributeType.INT,
        'completed_at': AttributeType.TIMESTAMP,
    },
    'data/raw/transactions': {
        'id': AttributeType.INT,
        'order_id': AttributeType.INT,
        'product_name': AttributeType.STRING,
        'unit_cost': AttributeType.DECIMAL,
        'quantity': AttributeType.INT,
    },
    'data/raw/customers': {
        'id': AttributeType.INT,
        'name': AttributeType.STRING,
    },
    'data/raw/shops': {
        'id': AttributeType.INT,
        'name': AttributeType.STRING,
        'country': AttributeType.STRING,
    }
}

raw_orders = Source('data/raw/orders')
raw_transactions = Source('data/raw/transactions')
raw_customers = Source('data/raw/customers')
raw_shops = Source('data/raw/shops')


class Shops(Derived):
    SOURCES = {
        'shops': Join(raw_shops, 'id')
    }

    @input('id', 'shops.id')
    @output(AttributeType.INT)
    def id(id):
        return id

    @input('name', 'shops.name')
    @output(AttributeType.STRING)
    def name(name):
        return name

    @input('country', 'shops.country')
    @output(AttributeType.STRING)
    def country_name(country):
        return country.lower()

    @input('country', 'shops.country')
    @output(AttributeType.STRING)
    def country_code(country):
        codes = {'united states': 'US',
                 'canada': 'CA'}
        return codes.get(country.lower())


class Customers(Derived):
    SOURCES = {
        'customers': Join(raw_customers, 'id')
    }

    @input('id', 'customers.id')
    @output(AttributeType.INT)
    def id(id):
        return id

    @input('name', 'customers.name')
    @output(AttributeType.STRING)
    def name(name):
        return name


class Sales(Derived):
    SOURCES = {
        'orders': Join(raw_orders, 'id'),
        'transactions': Join(raw_transactions, 'order_id'),
        'shops': Join(Shops(), 'id', match_name='shop_id'),
        'customers': Join(Customers(), 'id', match_name='customer_id')
    }

    @input('id', 'orders.id')
    @output(AttributeType.INT)
    def id(id):
        return id

    @input('id', 'orders.shop_id')
    @output(AttributeType.INT)
    def shop_id(id):
        return id

    @input('id', 'orders.customer_id')
    @output(AttributeType.INT)
    def customer_id(id):
        return id

    @input('name', 'shops.name')
    @output(AttributeType.STRING)
    def shop_name(name):
        return name

    @input('shop_name', 'shops.name')
    @input('customer_name', 'customers.name')
    @output(AttributeType.STRING)
    def shop_customer_name(shop_name, customer_name):
        return '{}:{}'.format(shop_name, customer_name)

shops = Shops()
customers = Customers()
sales = Sales()

SOURCES = (raw_orders, raw_transactions, raw_customers, raw_shops)
ENTITIES = (shops, customers, sales)

# --- ENGINE ---


class Graph:
    def __init__(self):
        self.edges = {}
        self.nodes = {}

    def add_node(self, key, val):
        self.nodes[key] = val

    def add_edge(self, from_key, to_key, meta=None):
        if from_key not in self.edges:
            self.edges[from_key] = []
        if [True for (t, _) in self.edges[from_key] if to_key == t]:
            return
        self.edges[from_key].append((to_key, meta))

    def in_edges(self, node_key):
        for from_key, outs in self.edges.items():
            for to_key, meta in outs:
                if to_key == node_key:
                    yield (from_key, meta)

    def dot(self):
        output = []
        output.append("digraph eavt {")
        for from_key, outs in self.edges.items():
            for to_key, _ in outs:
                output.append("    \"{}\" -> \"{}\"".format(from_key, to_key))
        output.append("}")
        return '\n'.join(output)


def build_entity_graph(sources, entities):
    graph = Graph()

    for source in sources:
        graph.add_node(source.name, source)

    for entity in entities:
        graph.add_node(entity.name, entity)
        for join_name, join in entity.SOURCES.items():
            meta = {
                'name': join_name,
                'join': join,
            }
            graph.add_edge(join.entity.name, entity.name, meta)

    return graph


def lookup_input_keys(entity_graph, entity_name, attr_key):
    input_alias, attr_name = attr_key.split('.')
    sources = entity_graph.in_edges(entity_name)

    input_joins = [
        join_def['join']
        for _, join_def in sources
        if join_def['name'] == input_alias
    ]
    assert len(input_joins) == 1
    input_join = input_joins[0]
    input_entity = input_join.entity

    input_key = '{}.{}'.format(input_entity.name, attr_name)
    parent_key = '{}.{}'.format(entity_name, input_join.match_name)
    return (input_key, parent_key)


def build_attribute_graph(entity_graph):
    attr_graph = Graph()

    for entity in entity_graph.nodes.values():
        for attribute in entity.attributes.values():
            key = '{}.{}'.format(entity.name, attribute.name)
            attr_graph.add_node(key, attribute)

            for inp in attribute.inputs.values():
                input_key, parent_key = lookup_input_keys(entity_graph, entity.name, inp)
                attr_graph.add_edge(input_key, key)
                if parent_key != key:
                    attr_graph.add_edge(parent_key, key)

    return attr_graph

entity_graph = build_entity_graph(SOURCES, ENTITIES)
attr_graph = build_attribute_graph(entity_graph)
