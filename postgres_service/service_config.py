import yaml
from postgres_service.service_db import PSQLServiceDB
from postgres_service.postgres_service import RWPostgresService
from postgres_service import utils
from openbrokerapi.catalog import Service, ServicePlan


class ServiceConfig(object):
    def __init__(self,
                 config_path,
                 secrets_path):
        self.config_path = config_path

        try:
            with open(self.config_path) as f:
                self.data = yaml.load(f)
        except IOError as (errno, strerror):
            raise IOError("I/O error({0}): {1}".format(errno, strerror))

        self.secrets = {}
        with open(secrets_path) as f:
            d = yaml.load(f)
            for s in d:
                self.secrets[d['id']] = s

    def get_services(self):
        # secrets for the internal database
        isecret = self.data['vaultkeeper_secrets']['internal_database']
        s = self.secrets[isecret]
        db, host, port = utils.parse_endpoint(s['endpoint'])
        username = s['username']
        password = s['password']
        internal_db = PSQLServiceDB(db, host, port, username, password)
        plans = self.data['plans']
        vault_addr = self.data['vault_addr']

        service_plans = {}
        for entry in self.data['plans']:
            plan = ServicePlan(id=entry['id'],
                               name=entry['name'],
                               description=entry['description'])
            service_plans['id'] = plan

        services = {}
        for entry in self.data['services']:
            s = self.secrets[entry['vaultkeeper_secrets']['vault_token']]
            token = s['token_value']
            catalog_entry = self._parse_service(entry, service_plans)
            cls = entry['class']
            instance = cls(token,
                           internal_db,
                           plans,
                           vault_addr,
                           catalog_entry,
                           )
            services[entry['id']] = instance
        return services

    def _parse_service(self, data, service_plans):
        return Service(id=data['id'],
                       name=data['name'],
                       description=data['description'],
                       bindable=data['bindable'],
                       plans=[p for k, p in service_plans.items() if k in data['plans']])


classnames = {
    'RWPostgresService': RWPostgresService,
}
