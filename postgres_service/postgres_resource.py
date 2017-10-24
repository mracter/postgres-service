import secrets
from twisted.enterprise import adbapi
from twistar.registry import Registry
from twisted.internet import reactor
from psycopg2.extras import DictCursor
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


class PostgreSQLDB(object):
    def __init__(self,
                 db_name,
                 db_host,
                 db_port,
                 db_user,
                 db_pass
                 ):
        self.db_name = db_name
        self.db_host = db_host
        self.db_port = db_port
        self.db_user = db_user
        self.db_pass = db_pass

    def provision(self, db_name):
        self._create_database(db_name)
        username, password = self._setup_database(db_name)
        return username, password

    def deprovision(self, db_name):
        self._drop_database(db_name)

    def _setup_database(self, db_name):
        self._connect_to_new_db(db_name)
        username, password = self._add_vault_user(db_name,
                                                  vault_user='vault')
        self._add_owner_role(db_name, role_name='db_owner')
        self._grant_owner(role_name='db_owner')
        return username, password

    def _create_database(self, db_name):
        self._connect_to_base_db()
        create_db = 'CREATE DATABASE {0} WITH OWNER DEFAULT'.format(db_name)
        Registry.DBPOOL.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        Registry.DBPOOL.runQuery(create_db).addCallback(self._done_query)
        reactor.run()

    def _drop_database(self, db_name):
        self._connect_to_base_db()
        create_db = 'DROP DATABASE %s  ;' % db_name
        Registry.DBPOOL.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        Registry.DBPOOL.runQuery(create_db).addCallback(self._done_query)
        reactor.run()

    def _add_vault_user(self,
                        db_name,
                        vault_user):
        password = self._gen_password()
        add_vuser = ('CREATE ROLE {0} WITH LOGIN ENCRYPTED PASSWORD {1} CREATEROLE, CREATEDB;'
                     'GRANT ALL PRIVILEGES ON DATABASE {2} TO vault WITH GRANT OPTION;'.format(vault_user, password,
                                                                                               db_name))
        Registry.DBPOOL.runQuery(add_vuser).addCallback(self._done_query)
        return vault_user, password

    def _add_owner_role(self,
                        db_name,
                        role_name):
        add_owner = ('CREATE ROLE db_owner;'
                     'ALTER DATABASE {0} OWNER TO {1};'
                     'ALTER SCHEMA public OWNER TO {1};'.format(db_name, role_name))
        Registry.DBPOOL.runQuery(add_owner).addCallback(self._done_query)

    def _grant_owner(self,
                     role_name):
        grant_owner = ('GRANT {0} TO vault;'.format(role_name))
        Registry.DBPOOL.runQuery(grant_owner).addCallback(self._done_query)

    def _connect_to_base_db(self):
        Registry.DBPOOL = self._get_connection(host=self.db_host,
                                               port=self.db_port,
                                               user=self.db_user,
                                               password=self.db_pass,
                                               db=self.db_name)

    def _connect_to_new_db(self, db_name):
        Registry.DBPOOL = self._get_connection(host=self.db_host,
                                               port=self.db_port,
                                               user=self.db_user,
                                               password=self.db_pass,
                                               db=db_name)

    def _done_query(self, result):
        print('Executed command with result ' + result)

    def _get_connection(self, db, host, port, user, password):
        return adbapi.ConnectionPool(
            'psycopg2',
            database=db,
            host=host,
            port=port,
            user=user,
            password=password,
            cp_min=1,
            cp_max=2,
            cursor_factory=DictCursor)

    def _gen_password(self,
                      length=30,
                      charset='ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!@#$%^&*()'):
        return ''.join([secrets.choice(charset) for _ in range(0, length)])
