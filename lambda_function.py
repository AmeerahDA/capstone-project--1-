import os # to store sensitive credentials  
import requests # to get url
import snowflake.connector as sf
import toml # to handle the configuration file

def load_config():
   # Load the configuration from the 'config.toml' file.
    app_config = toml.load('config.toml')
    return app_config

def get_snowflake_credentials():
    
    #Load the Snowflake connection parameters from environment variables.
   
    user = os.environ['user']
    password = os.environ['password']
    account = os.environ['account']
    warehouse = os.environ['warehouse']
    database = os.environ['database']
    schema = os.environ['schema']
    role = os.environ['role']
    return user, password, account, warehouse, database, schema, role

def download_file(url, destination_folder, file_name):

   # Download the file from the provided URL and save it to the destination folder.
   
    response = requests.get(url)
    response.raise_for_status()

    file_path = os.path.join(destination_folder, file_name)
    with open(file_path, 'wb') as file:
        file.write(response.content)

    with open(file_path, 'r') as file:
        file_content = file.read()
        print("File Content:")
        print(file_content)

    return file_path

def connect_to_snowflake(user, password, account, warehouse, database, schema, role):
    
    # Establish a connection to the Snowflake database.
   
    conn = sf.connect(user=user, password=password,
                     account=account, warehouse=warehouse,
                     database=database, schema=schema, role=role)
    cursor = conn.cursor()
    return conn, cursor

def create_file_format(cursor, file_format_name):
   
   # Create a CSV file format in Snowflake.
   
    create_csv_format = f"CREATE or REPLACE FILE FORMAT {file_format_name} TYPE ='CSV' FIELD_DELIMITER = ',';"
    cursor.execute(create_csv_format)

def create_stage(cursor, stage_name, file_format_name):
    
    # Create a Snowflake stage for the file.
   
    create_stage = f"CREATE OR REPLACE STAGE {stage_name} FILE_FORMAT ={file_format_name};"
    cursor.execute(create_stage)

def upload_file_to_stage(cursor, file_path, stage_name):
    
    # Upload the file to the Snowflake stage.
    
    run_put_statement = f"PUT 'file://{file_path}' @{stage_name};"
    cursor.execute(run_put_statement)

def list_stage_contents(cursor, stage_name):
    
   # List the contents of the Snowflake stage.
    
    list_stage = f"LIST @{stage_name};"
    cursor.execute(list_stage)

def truncate_table(cursor, schema, table):
    
   # Truncate the target Snowflake table.
    
    truncate_table = f"TRUNCATE TABLE {schema}.{table};"
    cursor.execute(truncate_table)

def copy_to_table(cursor, schema, table, stage_name, file_format_name, file_name):
    
    # Copy the file from the Snowflake stage to the target table.
    
    copy_into_query = f"COPY INTO {schema}.{table} FROM @{stage_name}/{file_name} FILE_FORMAT ={file_format_name} on_error='continue';"
    cursor.execute(copy_into_query)

def lambda_handler(event, context):
    """
    The main function that orchestrates the entire process.
    """
    app_config = load_config()

    url = app_config['url']
    destination_folder = app_config['destination_folder']
    file_name = app_config['file_name']
    file_format_type = app_config['file_format_type']
    stage_name = app_config['stage_name']
    table_name = app_config['table_name']

    user, password, account, warehouse, database, schema, role = get_snowflake_credentials()

    file_path = download_file(url, destination_folder, file_name)

    conn, cursor = connect_to_snowflake(user, password, account, warehouse, database, schema, role)

    create_file_format(cursor, file_format_type)
    create_stage(cursor, stage_name, file_format_type)
    upload_file_to_stage(cursor, file_path, stage_name)
    list_stage_contents(cursor, stage_name)
    truncate_table(cursor, schema, table_name)
    copy_to_table(cursor, schema, table_name, stage_name, file_format_type, file_name)

    print("File uploaded to Snowflake successfully.")

    return {
        'statusCode': 200,
        'body': 'File downloaded and uploaded to Snowflake successfully.'
    }
    