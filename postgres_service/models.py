from twistar.dbobject import DBObject


class Host(DBObject):
    pass


class PostgreSQLHost(DBObject):
    HASMANY = [{'name': 'pg_servers',
                'class_name': 'PostgreSQLServer',
                'foreign_key': ['host']}]


class PostgreSQLServer(DBObject):
    HASMANY = [{'name': 'pg_dbs',
                'class_name': 'PostgreSQLDatabase',
                'foreign_key': ['host', 'port']}]


class PostgreSQLDatabase(DBObject):
    HASMANY = [{'name': 'pg_bindings',
                'class_name': 'PostgreSQLBinding',
                'foreign_key': ['instance_id']}]


class PostgreSQLBinding(DBObject):
    pass
