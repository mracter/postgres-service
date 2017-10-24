from postgres_service.service_config import ServiceConfig
from os import environ


def main():
    cfg = environ.get('CONFIG_PATH', '')
    secrets_path = environ.get('CREDENTIAL_PATH', '')
    service_config = ServiceConfig(cfg, secrets_path)
    services = service_config.get_services()
    for name, service in services.items():
        service.app.run()


if __name__ == '__main__':
    main()
