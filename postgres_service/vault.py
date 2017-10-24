import hvac
import requests


class VaultMgr(object):
    def __init__(self,
                 token=None,
                 vault_addr=None,
                 gatekeeper_addr=None):
        self.vault_client = None
        self.vault_addr = vault_addr
        self.gatekeeper_addr = gatekeeper_addr
        self.token = token

    def setup(self):
        self.vault_client = hvac.Client(url=self.vault_addr)
        self.vault_client.token = self.token
        assert self.vault_client.is_authenticated()

    def add_gatekeeper_policy(self, policy):
        assert self.vault_client.is_authenticated()
        response = self.vault_client.read(self.vault_addr + '/secret/gatekeeper')
        p = response.json()['data']
        p.append(policy)
        response = self.vault_client.write(self.vault_addr + '/secret/gatekeeper',
                                           json=p)
        requests.post(self.gatekeeper_addr + '/policies/reload')
        return response

    def remove_gatekeeper_policy(self, key):
        assert self.vault_client.is_authenticated()
        response = self.vault_client.read(self.vault_addr + '/secret/gatekeeper')
        p = response.json()['data']
        if key in p:
            del p[key]
        else:
            return
        return self.vault_client.write(self.vault_addr + '/secret/gatekeeper',
                                       json=p)

    def refresh_gatekeeper_policy(self):
        requests.post(self.gatekeeper_addr + '/policies/reload')

    def _unwrap_token(self, wrapped_token):
        self.vault_client.unwrap(wrapped_token)
        if not self.vault_client.is_authenticated():
            raise RuntimeError('The service could not authenticate'
                               + 'to Vault with the unwrapped token.')
        return self.vault_client.token

    def get_db_server_connection(self,
                                 hostname,
                                 port):
        assert self.vault_client.is_authenticated()
        return self.vault_client.read(self.vault_addr +
                                      '/secret/databases/services/postgres/'
                                      '{0}/{1}'.format(hostname, port))

    def add_client_db_connection(self,
                                 conn_name,
                                 db_plugin,
                                 host,
                                 port,
                                 db_name,
                                 username,
                                 password,
                                 allowed_roles):
        assert self.vault_client.is_authenticated()
        json = {
            'plugin_name': db_plugin,
            'allowed_roles': allowed_roles,
            'connection_url': '{0}:{1}@{2}:{3}/{4}'.format(username,
                                                           password,
                                                           host,
                                                           port,
                                                           db_name)
        }
        return self.vault_client.write(self.vault_addr + '/database/config/' + conn_name,
                                       json=json)

    def get_client_db_connection(self,
                                 conn_name):
        assert self.vault_client.is_authenticated()
        return self.vault_client.read(self.vault_addr + '/database/config/' + conn_name)

    def delete_client_db_connection(self,
                                    conn_name):
        return self.vault_client.delete('/database/config/{0}'.format(conn_name))

    def add_database_role(self, conn_name, policy_name):
        json = {'db_name': conn_name,
                'creation_statements': 'CREATE ROLE \'{{name}}\' WITH LOGIN ENCRYPTED PASSWORD \'{{password}}\''
                                       'VALID UNTIL \'{{expiration}}\' IN ROLE \'db_owner\' '
                                       'INHERIT NOCREATEROLE NOCREATEDB NOSUPERUSER NOREPLICATION;'
                                       'GRANT USAGE ON SCHEMA public TO \'{{name}}\'; '
                                       'GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO \'{{name}}\';'
                                       'GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO \'{{name}}\';',
                'revocation_statements': 'REVOKE ALL PRIVILEGES ON ALL TABLES'
                                         'IN SCHEMA public FROM \'{{name}}\';'
                                         'REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public from \'{{name}}\';'
                                         'REVOKE ALL PRIVILEGES ON SCHEMA public FROM \'{{name}}\'; '
                                         'DROP ROLE IF EXISTS \'{{name}}\';',
                'default_ttl': '30s',
                }
        path = 'database/roles/{0}'.format(policy_name)
        return self.vault_client.write(path=path, json=json)

    def remove_database_role(self, instance_id):
        return self.vault_client.delete('/database/roles/psql-readwrite-{0}'.format(instance_id))
