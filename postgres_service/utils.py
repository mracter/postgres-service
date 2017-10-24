def parse_endpoint(endpoint):
    schema, rest = endpoint.split(':', 1)
    assert rest.startswith('//'), "DB URIs must start with scheme:// -- scheme not included / (in %r)" % rest
    rest = rest[2:]
    if rest.find('/') != -1:
        host, rest = rest.split('/', 1)
    else:
        raise ValueError("You MUST specify a database in the DB URI.")

    if host and host.find('@') != -1:
        user, host = host.split('@', 1)
    else:
        raise ValueError("You MUST specify a host in the DB URI.")

    if host and host.find(':') != -1:
        host, port = host.split(':')
        try:
            port = int(port)
        except ValueError:
            raise ValueError("port must be integer, got '%s' instead" % port)
        if not (1 <= port <= 65535):
            raise ValueError("port must be integer in the range 1-65535, got '%d' instead" % port)
    else:
        port = None
    db = rest
    return db, host, port
