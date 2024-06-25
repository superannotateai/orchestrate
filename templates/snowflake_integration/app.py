"""
Python Version Compatibility: 3.8, 3.9, 3.10, 3.11
Dependencies:
superannotate library, version 4.4.22 or higher
snowflake library, version 0.10.0 or higher
cryptography library, version 42.0.8 or higher

This action receives all the annotation data of the provided item IDs, transforms it, and inserts it into the Snowflake table.
Before running the script, make sure to set the necessary environment key-value variables. You can define these variables from the 'Secrets' page of the 'Actions' tab in Orchestrate.
You can then mount them in the pipeline.
Please refer to the documentation for more details: https://doc.superannotate.com/docs/create-automation#secret

In the current implementation, the SA_TOKEN and the Snowflake Private Key (key-pair authentication) are stored in the Secrets.

You also need to provide the following function arguments in the 'Event object' while setting up a pipeline:

- SA_COMPONENT_IDS: String with component IDs, separated by commas, which should move to Snowflake
- SNOWFLAKE_USERNAME: Snowflake username
- SNOWFLAKE_ACCOUNT: Snowflake account (must be {org_id}-{account_id} e.g. myorg-account123)
- SNOWFLAKE_WAREHOUSE: Snowflake warehouse
- SNOWFLAKE_DATABASE: Snowflake DB name
- SNOWFLAKE_DATABASE_SCHEMA: Snowflake schema name
- SNOWFLAKE_DB_TABLE_PATH: <Snowflake DB>.<Snowflake Schema>.<Snowflake>
- SNOWFLAKE_DB_COLUMN_NAMES: String of column names, separated by commas, which should receive the values from SuperAnnotate (order should match)


The 'handler' function triggers the script upon an event [Fired in Explore].
"""

import os

from snowflake import connector
from superannotate import SAClient
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization


def validate_context(context: dict):
    if context is None:
        raise Exception("Empty context")
    items = context.get('items')
    if not items:
        raise Exception("Invalid items list in context")

    project_id = context.get('project_id')
    team_id = context.get('team_id')

    if not project_id or not team_id:
        raise Exception("Invalid context")

    return {
        'items': items,
        'project_id': project_id,
        'team_id': team_id
    }


def find_componets_values(annotation, component_ids) -> dict:
    """
    return component_id -> value mapping
    """
    componnet_id_value_map = {}
    all_ids = component_ids.copy()
    for component_id in all_ids:
        for instance in annotation.get("instances", []):
            if "element_path" in instance.keys():
                id_in_path = instance.get("element_path")[0]
                if id_in_path is not None and id_in_path == component_id:
                    value = instance['attributes'][0]['name']
                    componnet_id_value_map[component_id] = value
        if component_id not in componnet_id_value_map:
            componnet_id_value_map[component_id] = ''
    return componnet_id_value_map


def argument_parser(event, arguments):
    arg_values = []
    for argument in arguments:
        arg_values.extend(argument_str_to_list(event.get(argument)))
    return arg_values


def argument_str_to_list(arg_str):
    arg_list_tmp = arg_str.split(",")
    arg_list = [s.strip() for s in arg_list_tmp]
    return arg_list


def get_db_connection(database, username, private_key, account, warehouse, schema='public'):
    return connector.connect(
        database=database,
        user=username,
        private_key=private_key,
        warehouse=warehouse,
        account=account,
        schema=schema
    )


def db_disconnect(connection, cursor):
    cursor.close()
    connection.close()


def db_create_insert_query(table_name, column_names, values_rows):
    if not values_rows:
        return False
    sql_terms = [f'INSERT INTO {table_name} ({", ".join(column_names)}) VALUES']
    value_placeholders = []

    for row in values_rows:
        placeholders = ["%s" if value not in [None, ''] else "NULL" for value in row]
        value_placeholders.append(f"({', '.join(placeholders)})")
    sql_terms.append(', '.join(value_placeholders))
    query = ' '.join(sql_terms)
    params = []
    for row in values_rows:
        params.extend([value for value in row if value not in [None, '']])
    return query, params


def generate_snowflake_pkb(private_key):
    p_key = serialization.load_pem_private_key(
        private_key.encode(),
        password=None,
        backend=default_backend()
    )

    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption())
    return pkb


def handler(event, context):
    sa_data = validate_context(context)

    sa = SAClient()

    sa_component_ids = argument_parser(event, ['SA_COMPONENT_IDS'])

    snowflake_username = argument_parser(event, ['SNOWFLAKE_USERNAME'])[0]
    snowflake_pkb = generate_snowflake_pkb(os.environ["SNOWFLAKE_PRIVATE_KEY"])
    snowflake_account = argument_parser(event, ['SNOWFLAKE_ACCOUNT'])[0]
    snowflake_warehouse = argument_parser(event, ['SNOWFLAKE_WAREHOUSE'])[0]
    snowflake_db = argument_parser(event, ['SNOWFLAKE_DATABASE'])[0]
    snowflake_db_schema = argument_parser(event, ['SNOWFLAKE_DATABASE_SCHEMA'])[0]
    snowflake_db_table_path = argument_parser(event, ['SNOWFLAKE_DB_TABLE_PATH'])[0]
    snowflake_db_column_names = argument_parser(event, ['SNOWFLAKE_DB_COLUMN_NAMES'])

    db_connection = get_db_connection(
        database=snowflake_db, username=snowflake_username, account=snowflake_account,
        private_key=snowflake_pkb, warehouse=snowflake_warehouse, schema=snowflake_db_schema
    )
    db_cursor = db_connection.cursor()
    try:
        annotation_data = sa.get_annotations(sa_data['project_id'], items=sa_data['items'])

        sql_values = []
        for annotation in annotation_data:
            componnet_id_value_map = find_componets_values(annotation, sa_component_ids)
            if componnet_id_value_map:
                sql_values.append(componnet_id_value_map.values())
        if sql_values:
            query, params = db_create_insert_query(snowflake_db_table_path, snowflake_db_column_names, sql_values)
            if query:
                db_cursor.execute(query, params)
                print(db_cursor.fetchall())
        return True
    finally:
        db_connection.close()
