import psycopg2
import pyodbc
import datetime

def connect_sql_server(server, database, user, password):
    return pyodbc.connect(
        f'DRIVER={{ODBC Driver 17 for SQL Server}};'
        f'SERVER={server};'
        f'DATABASE={database};'
        f'UID={user};'
        f'PWD={password};'
    )

def connect_postgresql(dbname, user, password, host, port):
    return psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port
    )

def create_sql_server_table(conn, table_name, columns):
    cursor = conn.cursor()
    cursor.execute(f'DROP TABLE IF EXISTS {table_name};')
    cursor.execute(f'CREATE TABLE {table_name} ({columns});')
    conn.commit()
    cursor.close()

def copy_data_postgresql_to_sql_server_all(conn_pg, conn_sql_server, table_name):
    cursor_pg = conn_pg.cursor()
    cursor_sql_server = conn_sql_server.cursor()

    cursor_pg.execute(f'SELECT * FROM {table_name};')
    data = cursor_pg.fetchall()

    for row in data:
        print (f"{row}")
        cursor_sql_server.execute(f'INSERT INTO {table_name} VALUES ({", ".join("?" * len(row))});', row)

    conn_sql_server.commit()
    print(f"{table_name} successful")
    cursor_pg.close()
    cursor_sql_server.close()

def copy_data_postgresql_to_sql_server(conn_pg, conn_sql_server, table_name, selected_columns):
    cursor_pg = conn_pg.cursor()
    cursor_sql_server = conn_sql_server.cursor()

    select_columns = ', '.join(selected_columns)
    cursor_pg.execute(f'SELECT {select_columns} FROM {table_name} WHERE ;')
    data = cursor_pg.fetchall()

    for row in data:
        print(f"{row}")
        cursor_sql_server.execute(f'INSERT INTO {table_name} ({", ".join(selected_columns)}) VALUES ({", ".join("?" * len(selected_columns))});', row)

    conn_sql_server.commit()
    print(f"Data from {table_name} copied successfully.")
    cursor_pg.close()
    cursor_sql_server.close()

def copy_data_postgresql_to_sql_server_batch(conn_pg, conn_sql_server, table_name, selected_columns, batch_size=1000, where_clause=""):
    cursor_pg = conn_pg.cursor()
    cursor_sql_server = conn_sql_server.cursor()

    select_columns = ', '.join(selected_columns)
    cursor_pg.execute(f'SELECT {select_columns} FROM {table_name} {where_clause};')

    while True:
        batch = cursor_pg.fetchmany(batch_size)
        if not batch:
            break

        values_list = []
        for row in batch:
            values_list.append(row)

        insert_sql = format_insert_values(table_name, selected_columns, values_list)
        print(insert_sql)
        cursor_sql_server.execute(insert_sql)

    print(f"Data from {table_name} copied successfully.")

    conn_sql_server.commit()
    cursor_pg.close()
    cursor_sql_server.close()

def format_insert_values(table_name, column_list, value_lists):
    columns = ', '.join(column_list)
    final_values = ""

    for values in value_lists:
        values_str = "('{}')".format("', '".join([str(value).replace("'", "''") for value in values]))
        values_str = values_str + ",\n"
        final_values = final_values + values_str

    final_values = final_values.rstrip(',\n') + ";"
    sql_insert = f'INSERT INTO {table_name} ({columns})\nVALUES\n{final_values}'
    return sql_insert

def add_single_quotes_if_needed(input_string):
    if "'" in input_string:
        input_string = "'" + input_string + "'"
    return input_string

def lambda_handler(event, context):
   # AWS RDS PostgreSQL Database Configuration
    postgres_host = os.environ.get('POSTGRES_HOST')
    postgres_port = os.environ.get('POSTGRES_PORT')
    postgres_dbname = os.environ.get('POSTGRES_DBNAME')
    postgres_user = os.environ.get('POSTGRES_USER')
    postgres_password = os.environ.get('POSTGRES_PASSWORD')

    # SQL Server Database Configuration
    sql_server_host = os.environ.get('SQL_SERVER_HOST')
    sql_server_dbname = os.environ.get('SQL_SERVER_DBNAME')
    sql_server_user = os.environ.get('SQL_SERVER_USER')
    sql_server_password = os.environ.get('SQL_SERVER_PASSWORD')

    # Set up Connections
    conn_pg = connect_postgresql(postgres_dbname, postgres_user, postgres_password, postgres_host, postgres_port)
    conn_sql_server = connect_sql_server(sql_server_host, sql_server_dbname, sql_server_user, sql_server_password)

    start_time = datetime.datetime.now()

    # Migrate TARGET_MAPPINGS table
    target_mapping_start_time = datetime.datetime.now()

    table_name = 'target_mappings'
    columns = 'lycos_id VARCHAR(255), arachnid_id VARCHAR(255), mozenda_id VARCHAR(255), migration_set INT, lycos_type VARCHAR(255)'
    selected_columns = ['lycos_id', 'arachnid_id', 'mozenda_id', 'migration_set','lycos_type']

    create_sql_server_table(conn_sql_server, table_name, columns)
    copy_data_postgresql_to_sql_server_batch(conn_pg, conn_sql_server, table_name, selected_columns)
    target_mapping_end_time = datetime.datetime.now()

    # Migrate TARGET table
    table_name = 'targets'
    columns = 'id VARCHAR(1024), url VARCHAR(1024), name VARCHAR(1024), type VARCHAR(1024), status bigint, spider VARCHAR(1024)'
    selected_columns = ['id', 'url', 'name','type','status','spider']

    create_sql_server_table(conn_sql_server, table_name, columns)
    copy_data_postgresql_to_sql_server_batch(conn_pg, conn_sql_server, table_name, selected_columns, 1000)

    target_end_time = datetime.datetime.now()

    # Migrate TARGET_CRAWLING_SESSION table, 2 15

    table_name = 'target_crawling_sessions'
    columns = 'target_id VARCHAR(1024), crawl_session_id VARCHAR(1024), postings_found int, seen_postings int, first_seen_timestamp VARCHAR(1024), scraped_date VARCHAR(1024), status INT'
    selected_columns = ['target_id', 'crawl_session_id', 'postings_found', 'seen_postings', 'first_seen_timestamp', 'scraped_date', 'status']
    #where_clause = "WHERE scraped_date >= '2023-08-01' and scraped_date <= '2023-09-11'"
    where_clause = ""

    create_sql_server_table(conn_sql_server, table_name, columns)
    copy_data_postgresql_to_sql_server_batch(conn_pg, conn_sql_server, table_name, selected_columns, 1000,where_clause)

    target_crawling_session_end_time = datetime.datetime.now()

    conn_pg.close()
    conn_sql_server.close()

    print(f"Start time: {start_time}")
    print(f"Targets mapping migration end time: {target_mapping_end_time}")
    print(f"Targets migration end time: {target_end_time}")
    print(f"Target crawling session end time: {target_crawling_session_end_time}")

    return {
        'statusCode': 200,
        'body': 'Data copied successfully!'
    }

if __name__ == "__main__":
    lambda_handler("","");


##
import json

#print('Loading function')


#def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))
#    print("value1 = " + event['key1'])
#    print("value2 = " + event['key2'])
#    print("value3 = " + event['key3'])
#    #return event['key1']  # Echo back the first key value
#    raise Exception('Something went wrong')




