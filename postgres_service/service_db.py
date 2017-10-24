# this is the internal service DAL
from twisted.enterprise import adbapi
from twistar.registry import Registry
from twisted.internet import reactor
from postgres_service.models import PostgreSQLDatabase, PostgreSQLHost, PostgreSQLBinding, PostgreSQLServer
import psycopg2
from psycopg2.extras import DictCursor


class PSQLServiceDB(object):
    hosts = ('CREATE TABLE hosts (host VARCHAR(256) UNIQUE, '
             'ip INET);')
    pg_servers = ('CREATE TABLE pg_servers ('
                  'FOREIGN KEY host REFERENCES hosts(host) ON DELETE CASCADE, '
                  'port INT);')
    pg_dbs = ('CREATE TABLE pg_dbs (db_name VARCHAR(256), '
              'instance_id UNIQUE VARCHAR(256), '
              'FOREIGN_KEY (host, port) REFERENCES pg_servers(host, port) ON DELETE CASCADE;')
    pg_bindings = ('CREATE TABLE pg_bindings (binding_id VARCHAR(256) UNIQUE, '
                   'instance_id VARCHAR(256) REFERENCES pg_dbs(instance_id) '
                   'ON DELETE CASCADE;')

    def __init__(self,
                 db_name,
                 db_host,
                 db_port,
                 db_user,
                 db_pass):
        self.db_name = db_name
        self.db_host = db_host
        self.db_port = db_port
        self.db_user = db_user
        self.db_pass = db_pass

    def setup(self):
        self._setup_db()
        Registry.DBPOOL = self._get_connection()

    def add_psql_instance(self,
                          host,
                          port,
                          db_name,
                          instance_id):
        database = PostgreSQLDatabase()
        database.db_name = db_name
        database.host = host
        database.port = port
        database.instance_id = instance_id
        database.save().addCallback(self._done_query)

    def add_binding(self,
                    instance_id,
                    binding_id):
        binding = PostgreSQLBinding()
        binding.instance_id = instance_id
        binding.binding_id = binding_id
        binding.save().addCallback(self._done_query)

    def get_hosts(self):
        hosts = PostgreSQLHost.all().addCallback(self._return_array)
        return hosts

    def get_servers(self):
        servers = PostgreSQLServer.all().addCallback(self._return_array)
        return servers

    def get_databases(self):
        databases = PostgreSQLDatabase.all().addCallback(self._return_array)
        return databases

    def get_bindings(self):
        bindings = PostgreSQLBinding.all().addCallback(self._return_array)
        return bindings

    def remove_binding(self, binding_id):
        binding = self.get_binding(binding_id)
        PostgreSQLBinding.delete(binding).addCallback(self._done_query)

    def get_instance(self, instance_id):
        instance = PostgreSQLDatabase.find(
                where=['instance_id = ?', instance_id],
                limit=1).addCallback(self._return_object)
        return instance

    def remove_instance(self, instance_id):
        instance = self.get_instance(instance_id)
        PostgreSQLDatabase.delete(instance).addCallback(self._done_query)

    def get_binding(self, binding_id):
        instance = PostgreSQLBinding.find(
                where=['instance_id = ?', binding_id],
                limit=1).addCallback(self._return_object)
        return instance

    def _get_connection(self):
        return adbapi.ConnectionPool(
            'psycopg2',
            database=self.db_name,
            host=self.db_host,
            port=self.db_port,
            user=self.db_user,
            password=self.db_pass,
            cp_min=1,
            cp_max=2,
            cursor_factory=DictCursor)

    # Database needs to already be populated with lists of hosts and databases
    def _setup_db(self):
        Registry.register(PostgreSQLHost, PostgreSQLDatabase)
        Registry.register()

        Registry.DBPOOL = self._get_connection()
        Registry.DBPOOL.runQuery(self.hosts).addCallback(self._done_query)
        Registry.DBPOOL.runQuery(self.pg_servers).addCallback(self._done_query)
        Registry.DBPOOL.runQuery(self.pg_dbs).addCallback(self._done_query)
        Registry.DBPOOL.runQuery(self.pg_bindings).addCallback(self._done_query)

        reactor.run()
        return

    def _done_query(self, result):
        print('Executed command with result ' + result)

    def _return_array(self, objects):
        return objects

    def _return_object(self, objects):
        return objects[0]


def ignore_pg_error(d, pgcode):
    '''
    Ignore a particular postgres error.
    '''

    def trap_err(f):
        f.trap(psycopg2.ProgrammingError)
        if f.value.pgcode != pgcode:
            return f

    return d.addErrback(trap_err)
