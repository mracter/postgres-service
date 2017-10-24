# this service should be Vaultkeepered
# uses internal postgres db as source of truth for database hosts and existing databases

import random
from klein import Klein

from postgres_service.vault import VaultMgr
from postgres_service.postgres_resource import PostgreSQLDB


class PostgresService(object):
    app = Klein()

    def __init__(self,
                 vault_token,
                 service_db,
                 plans,
                 vault_addr,
                 catalog_entry):
        self.vault_token = vault_token
        self.plans = plans
        self.vault_mgr = None
        self.service_db = service_db
        self.app = Klein()
        self.service_db = service_db
        self.vault_addr = vault_addr
        self.catalog_entry = catalog_entry

    def setup(self):
        pass

    def _select_db_server(self):
        pass

    def _get_policy_name(self, instance_id):
        pass

    def _instance_exists(self, instance_id) -> bool:
        databases = self.service_db.get_instances()
        return any(db for db in databases if db.instance_id == instance_id)

    def _binding_exists(self, binding_id) -> bool:
        bindings = self.service_db.get_bindings()
        return any(bd for bd in bindings if bd.binding_id == binding_id)

    def provision(self, instance_id):
        pass

    def deprovision(self, instance_id):
        pass

    def bind(self, instance_id, binding_id):
        pass

    def unbind(self, instance_id, binding_id):
        pass

    def get_service(self):
        return self.catalog_entry


# noinspection PyRedundantParentheses
class RWPostgresService(PostgresService):
    app = Klein()

    # instantiate Vault connection, get token from Vaultkeeper output
    def setup(self):
        self.vault_mgr = VaultMgr(token=self.vault_token)

    # add Vault connection here
    # provisioning means choosing a database from the list at random then
    # creating a PostgreSQLDB resource using the connection information. You get the info
    # by querying Vault
    @app.route('/postgres_rw/<instance_id>', method='PUT')
    def provision(self, instance_id):
        if self._instance_exists(instance_id):
            return (409, 'Resource already provisioned for the provided ID')

        server = self._select_db_server()
        hostname = server.hostname
        port = server.port
        conn = self.vault_mgr.get_db_server_connection(hostname, port)
        db = PostgreSQLDB(db_host=conn['host'],
                          db_port=conn['port'],
                          db_name=conn['db_name'],
                          db_user=conn['db_user'],
                          db_pass=conn['db_pass'])
        db_name = instance_id
        username, password = db.provision(instance_id)
        policy_name = self._get_policy_name(instance_id)
        connection_name = instance_id
        self.vault_mgr.add_client_db_connection(conn_name=connection_name,
                                                db_plugin='',
                                                host=conn['host'],
                                                port=conn['port'],
                                                db_name=db_name,
                                                username=username,
                                                password=password,
                                                allowed_roles=[policy_name])
        self.vault_mgr.add_database_role(connection_name, policy_name)
        self.service_db.add_psql_instance(host=conn['host'],
                                          port=conn['port'],
                                          db_name=db_name,
                                          instance_id=instance_id)
        return (200,
                {'instance_id': instance_id,
                 'state': 'provision_success',
                 'dashboard_url': 'http://localhost:8090/dashboard/' + instance_id})

    @app.route('/postgres_rw/<instance_id>', method='DELETE')
    def deprovision(self, instance_id):
        instance = self.service_db.get_instance(instance_id)
        db_name = instance.db_name
        host = instance.host
        port = instance.port
        conn = self.vault_mgr.get_db_server_connection(host, port)
        db = PostgreSQLDB(db_host=conn['host'],
                          db_port=conn['port'],
                          db_name=conn['db_name'],
                          db_user=conn['db_user'],
                          db_pass=conn['db_pass'])
        db.deprovision(db_name)
        self.service_db.remove_instance(instance_id)

    # binding_id is the app name
    @app.route('/postgres_rw/<instance_id>/<binding_id>', method='PUT')
    def bind(self, instance_id, binding_id):
        if self._binding_exists(binding_id):
            return (409, 'Binding already provisioned for the provided binding ID')
        if not self._instance_exists(instance_id):
            return (409, 'Instance does not exist for the provided instance ID')

        policy_name = self._get_policy_name(instance_id)
        gk_policy = {
            binding_id: {
                'policies': [policy_name],
                'ttl': 3000
            }
        }
        self.vault_mgr.add_gatekeeper_policy(gk_policy)

        instance = self.service_db.get_instance(instance_id)
        db_name = instance.db_name
        host = instance.host
        port = instance.port
        self.service_db.add_binding(instance_id,
                                    binding_id)
        cfg = [
            {
                'id': '{0}'.format(binding_id),
                'backend': 'postgresql',
                'endpoint': '{0}:{1}/{2}'.format(db_name, host, port),
                'vault_path': 'database/creds/{0}'.format(instance_id),
                'schema': 'public',
                'policy': 'psql-readwrite-{0}'.format(instance_id),
                'set_role': 'db_owner'
            }
        ]
        return (200, cfg)

    # actual revocation is done by killing the app
    @app.route('/postgres_rw/<instance_id>/<binding_id>', method='DELETE')
    def unbind(self, instance_id, binding_id):
        if self._binding_exists(binding_id):
            return (409, 'Binding already provisioned for the provided binding ID')
        if not self._instance_exists(instance_id):
            return (409, 'Instance does not exist for the provided instance ID')

        self.vault_mgr.remove_gatekeeper_policy(binding_id)
        self.service_db.remove_binding(instance_id,
                                       binding_id)

    # randomly select a database server in internal db
    def _select_db_server(self):
        db_servers = self.service_db.get_servers()
        server = int(random.random() * db_servers.length)
        return db_servers[server]

    def _get_policy_name(self, instance_id):
        return 'psql-readwrite-{0}'.format(instance_id)
