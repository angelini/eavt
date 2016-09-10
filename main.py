from enum import Enum
from functools import wraps


class ColumnNotFound(Exception):
    def __init__(self, table, column):
        self.message = '{}.{} not found: {}.\{{}\}' \
            .format(table, column, table, ', '.join(table.columns))


class ColumnType(Enum):
    INT = 1
    STRING = 2
    DECIMAL = 3
    TIMESTAMP = 4


class Column:
    def __init__(self, table, name, col_type):
        self.table = table
        self.name = name
        self.col_type = col_type

    def __repr__(self):
        return 'Column(Table({}), {}, {})' \
            .format(self.table.name, self.name, self.col_type)


class Table:
    def __init__(self, name, columns):
        self.name = name
        self.columns = columns

    def __repr__(self):
        column_names = [col_name for col_name in self.columns.keys()]
        return 'Table({}, {})'.format(self.name, column_names)


class Source(Table):
    def __init__(self, path):
        super().__init__(path, self._load_schema(path))

    def __getitem__(self, name):
        if name not in self.columns:
            raise ColumnNotFound(self, name)
        return Column(self, name, self.columns[name])

    @staticmethod
    def _load_schema(path):
        return SCHEMAS[path]


class Entity(Table):
    def __init__(self):
        name = self.__class__.__name__.lower()
        attributes = {}

        for attr_name in dir(self):
            class_attr = getattr(self, attr_name)
            if hasattr(class_attr, '_output'):
                attributes[attr_name] = class_attr._output

        super().__init__(name, attributes)


class Join:
    def __init__(self, table, col_name, entity_attribute='id'):
        self.table = table
        self.col_name = col_name
        self.entity_attribute = entity_attribute

    def __repr__(self):
        return 'Join({}, {}, {})' \
            .format(self.table, self.col_name, self.entity_attribute)


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


def output(col_type):
    def output_decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            return fn(*args, **kwargs)

        wrapper._output = col_type
        return wrapper

    return output_decorator

# --- DATA ---

SCHEMAS = {
    'data/raw/orders': {
        'id': ColumnType.INT,
        'shop_id': ColumnType.INT,
        'customer_id': ColumnType.INT,
        'completed_at': ColumnType.TIMESTAMP,
    },
    'data/raw/transactions': {
        'id': ColumnType.INT,
        'order_id': ColumnType.INT,
        'product_name': ColumnType.STRING,
        'unit_cost': ColumnType.DECIMAL,
        'quantity': ColumnType.INT,
    },
    'data/raw/customers': {
        'id': ColumnType.INT,
        'name': ColumnType.STRING,
    },
    'data/raw/shops': {
        'id': ColumnType.INT,
        'name': ColumnType.STRING,
        'country': ColumnType.STRING,
    }
}

raw_orders = Source('data/raw/orders')
raw_transactions = Source('data/raw/transactions')
raw_customers = Source('data/raw/customers')
raw_shops = Source('data/raw/shops')


class Shops(Entity):
    SOURCES = {
        'shops': Join(raw_shops, 'id')
    }

    @input('name', 'customers.name')
    @output(ColumnType.STRING)
    def name(name):
        return name

    @input('country', 'shops.country')
    @output(ColumnType.STRING)
    def country_name(country):
        return country.lower()

    @input('country', 'shops.country')
    @output(ColumnType.STRING)
    def country_code(country):
        codes = {'united states': 'US',
                 'canada': 'CA'}
        return codes.get(country.lower())


class Customers(Entity):
    SOURCES = {
        'customers': Join(raw_customers, 'id')
    }

    @input('name', 'customers.name')
    @output(ColumnType.STRING)
    def name(name):
        return name


class Sales(Entity):
    SOURCES = {
        'orders': Join(raw_orders, 'id'),
        'transactions': Join(raw_transactions, 'order_id'),
        'shops': Join(raw_shops, 'id', entity_attribute='shop_id'),
        'customers': Join(raw_customers, 'id', entity_attribute='customer_id')
    }

    @input('id', 'orders.shop_id')
    @output(ColumnType.INT)
    def shop_id(id):
        return id

    @input('id', 'orders.customer_id')
    @output(ColumnType.INT)
    def customer_id(id):
        return id

    @input('name', 'shops.name')
    @output(ColumnType.STRING)
    def shop_name(name):
        return name

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

    def add_edge(self, from_key, to_key, join):
        assert from_key in self.nodes
        assert to_key in self.nodes
        self.edges[from_key] = (to_key, join)


def build_graph(sources, entities):
    graph = Graph()

    for source in sources:
        graph.add_node(source.name, source)
    for entity in entities:
        graph.add_node(entity.name, entity)

    for entity in entities:
        for join in entity.SOURCES.values():
            graph.add_edge(join.table.name, entity.name, join)

    return graph


graph = build_graph(SOURCES, ENTITIES)
