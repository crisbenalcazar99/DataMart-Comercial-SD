def build_connection_url(config):

    engine = config.get('engine', 'postgresql')
    driver = config.get('driver', 'psycopg2')
    user = config['user']
    password = config['password']
    host = config['host']
    port = config['port']
    db = config.get('database', '')


    if not engine or not driver:
        raise ValueError("Faltan parámetros obligatorios: 'engine' o 'driver'")

    url = f"{engine}+{driver}://{user}:{password}@{host}:{port}"
    return f"{url}/{db}" if db else url
