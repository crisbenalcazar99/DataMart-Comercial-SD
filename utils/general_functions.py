def load_sql_statement(query_path):
    with open(query_path, 'r') as file:
        return file.read()

def list_in_string(list_objects):
    return tuple(f"'{value}'" for value in list_objects)

