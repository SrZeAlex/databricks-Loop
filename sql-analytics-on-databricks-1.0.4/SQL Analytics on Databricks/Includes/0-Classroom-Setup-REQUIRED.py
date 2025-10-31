# Databricks notebook source
# MAGIC %run ../../Includes/_common

# COMMAND ----------

DA = DBAcademyHelper()
DA.init()

# COMMAND ----------

## Create a volume in the user's schema
def create_volume_for_user(catalog: str, schema: str, volume_name: str) -> None:
    spark.sql(f'CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume_name}')
    print(f"Created the volume if it doesn't exist: {catalog}.{schema}.{volume_name}.")

# COMMAND ----------

## Functions to help with the setup of creating the tables in the user's schema

def get_tables_in_catalog(catalog: str, schema: str) -> list:
    """
    Fetch tables from a specific catalog and schema.

    Args:
        catalog (str): Name of the catalog
        schema (str): The name of the schema
        
    Returns:
        list: List of table names.

    Example:
        >>> get_tables_in_catalog(catalog = 'dbacademy_aus_sales', schema = 'v01') -> ['customers', 'opportunities', 'orders']
    """
    query = f'SHOW TABLES IN {catalog}.{schema}'

    tables_in_catalog = spark.sql(query).toPandas().tableName.to_list()

    append_catalog_schema_to_tables = [f'{catalog}.{schema}.{table}' for table in tables_in_catalog]

    return append_catalog_schema_to_tables



def get_country_name_from_catalog(catalog_name: str) -> str:
    """
    Extracts the country name (or country code) from a catalog name.
    
    The function assumes the catalog name is in the format:
    'dbacademy_<country_code>_sales', where <country_code> is the part 
    of the name between the first two underscores.
    
    Args:
        catalog_name (str): The name of the catalog in the format 'prefix_<country_code>_suffix'.
        
    Returns:
        str: The country code (the part of the catalog name between the first two underscores).
    
    Example:
        >>> get_country_name_from_catalog('dbacademy_ca_sales')  -> 'ca'
    """
    
    # Split the catalog name by underscores and return the part at index 1 (country code)
    return catalog_name.split('_')[1]



def rename_table_with_appended_country(append_value: str, full_table_name: str) -> str:
    '''
    Renames a table by appending a country code or identifier to its name.

    This function takes a full table name (in the format "schema.database.table") and extracts
    the table's name from it. It then appends the provided value (usually a country code or identifier)
    to the table name and returns the new name.

    Args:
        append_value (str): The value to append to the table's name (e.g., a country code like "US" or "UK").
        full_table_name (str): The full name of the table, typically in the format "catalog.schema.table".

    Returns:
        str: The new table name with the appended value without the catalog and schema.

    Example:
        >>> rename_table_with_appended_country("US", "schema.database.users")
        'US_users'
    '''
    table_name = full_table_name.split('.')[2]

    new_table_name = f'{append_value}_{table_name}'

    return new_table_name


def create_table_for_user(in_catalog: str, in_schema: str, new_table_name: str, from_table: str) -> None:
    '''
    Creates a new user-specific table in the specified catalog and schema by copying data from an existing table.

    This function drops the user table if it already exists and creates a new table using the provided 
    `from_table` as the source. The new table is named based on the `new_table_name` and is stored 
    in the specified catalog and schema.

    Args:
        in_catalog (str): The catalog in which the new table will be created.
        in_schema (str): The schema where the new table will reside.
        new_table_name (str): The name of the new table to be created.
        from_table (str): The name of the existing table from which data will be copied. Needs the full    table name: 'catalog.schema.table'

    Returns:
        None: This function does not return a value but prints a message indicating the table creation process.

    Example:
        >>> create_table_for_user('my_catalog', 'user_schema', 'user_data', 'source_table')
        Creating the user table: my_catalog.user_schema.user_data, from: source_table
    '''   
    create_user_table = f'{in_catalog}.{in_schema}.{new_table_name}'
    drop = spark.sql(f'DROP TABLE IF EXISTS {create_user_table}')

    r = spark.sql(f'''
            CREATE TABLE IF NOT EXISTS {create_user_table} AS 
            SELECT * 
            FROM {from_table}
        ''')
    print(f'Creating the user table: {create_user_table}, from: {from_table}')


def print_setup_complete():
    '''
    Simple printing of a note after the completion of the setup.
    '''
    print('\n-----------------------------------------')
    print('COURSE SETUP COMPLETE!')
    print('-----------------------------------------')

# COMMAND ----------

def create_tables_in_user_schema(user_catalog: str, 
                                 user_schema: str, 
                                 catalogs_to_search: list):

    ## Search for all tables in the specified catalogs and return a list of tables for each catalog
    for search_catalog in catalogs_to_search:
        tables_in_catalog = get_tables_in_catalog(catalog = search_catalog, schema = 'v01')


        ## Create a new table name with country_tablename
        for full_src_table_name in tables_in_catalog:
            new_full_table_name = rename_table_with_appended_country(append_value = get_country_name_from_catalog(search_catalog),
                                                                     full_table_name = full_src_table_name)

            create_table_for_user(in_catalog='databrickstheloop', 
                                  in_schema=DA.schema_name, 
                                  new_table_name=new_full_table_name, 
                                  from_table=full_src_table_name)

# COMMAND ----------

def create_csv_file(write_path: str, 
                    file_name: str, 
                    header: list[str],
                    data: list[tuple]):
    
    '''
    # Define the header
    header = ['productid', 'productname', 'listprice']

    # Data to be written to CSV
    data = [
        ('JQ9322', 'TableSaw', 24.99),
        ('A802a', 'Mill', 10.17),
        ('EE05x', 'Press', 363.02),
        ('R9S10', 'Lathe', 43.83),
        ('BC111d', 'Drill', 63.28),
        ('W8931', 'Planer', 45.22),
        ('Q28R3', 'Grinder', 176.33),
        ('MDb304', 'CAD System', 502.80),
        ('OR307p', 'Kazoo', 0.99)
    ]
    '''
    
    import csv

    csv_file = f'{write_path}/{file_name}.csv'
    print(csv_file)

    # Write to CSV file
    with open(csv_file, mode='w', newline='') as file:
        writer = csv.writer(file)
        if header is not None: # Write header if not none
            writer.writerow(header)  
        writer.writerows(data)   # Write data rows

    print(f"CSV file {csv_file} created successfully.")

# COMMAND ----------

def create_au_products_lookup_table(schema_name=DA.schema_name):
    drop = spark.sql(f'DROP TABLE IF EXISTS databrickstheloop.{schema_name}.au_products_lookup')

    tbl = spark.sql(f'''
        CREATE TABLE databrickstheloop.{schema_name}.au_products_lookup AS
        SELECT productid, productname, listprice
        FROM read_files(
            '/Volumes/databrickstheloop/{schema_name}/backup/au_products.csv',
            format => 'csv',
            header => true)
    ''')
    print(f'Created the table databrickstheloop.{schema_name}.au_products_lookup.')

# COMMAND ----------

## Get path from where the notebook is run
def return_current_directory():
    from pathlib import Path
    current_directory = Path().resolve()

    return current_directory

# COMMAND ----------

## Drops the temp view to avoid errors
drop_temp_view = spark.sql('DROP VIEW IF EXISTS _sqldf')

## Creates tables by copying everything from the catalogs_to_search
create_tables_in_user_schema(user_catalog = 'databrickstheloop', 
                             user_schema = DA.schema_name, 
                             catalogs_to_search=['dbacademy_aus_sales', 'dbacademy_ca_sales'])


## Create volumes in the user's schema
create_volume_for_user(catalog='databrickstheloop', schema=DA.schema_name, volume_name='myfiles')
create_volume_for_user(catalog='databrickstheloop', schema=DA.schema_name, volume_name='backup')

## Create the CSV file in the volume and the /M02 - Data Importing/data folder. Backup is if they can't upload the file manually.
path = return_current_directory()
    
###########################
# create au_products.csv
###########################
# Define the header
au_prod_header = ['productid', 'productname', 'listprice']

# Data to be written to CSV
au_prod_data = [
    ('JQ9322', 'TableSaw', 24.99),
    ('A802a', 'Mill', 10.17),
    ('EE05x', 'Press', 363.02),
    ('R9S10', 'Lathe', 43.83),
    ('BC111d', 'Drill', 63.28),
    ('W8931', 'Planer', 45.22),
    ('Q28R3', 'Grinder', 176.33),
    ('MDb304', 'CAD System', 502.80),
    ('OR307p', 'Kazoo', 0.99)
]

# COMMAND ----------

path = "/Workspace/Users/samantha.cruz@theloop.pt/sql-analytics-on-databricks-1.0.4/SQL Analytics on Databricks/M02 - Data Importing/data"

# COMMAND ----------

create_csv_file(write_path = f'{path}', 
                file_name = 'au_products', 
                header = au_prod_header,
                data = au_prod_data)
    
create_csv_file(write_path = f'/Volumes/databrickstheloop/{DA.schema_name}/backup', 
                file_name = 'au_products', 
                header = au_prod_header,
                data = au_prod_data)

## Create au_products lookup table for demos
create_au_products_lookup_table()

# COMMAND ----------

print_setup_complete()
