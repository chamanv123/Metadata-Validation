import snowflake.connector
import ibm_db
import pandas as pd

def get_snowflake_data(sf_config, table_name, pk_columns):
    """Fetch data from Snowflake."""
    conn = snowflake.connector.connect(
        user=sf_config['user'],
        password=sf_config['password'],
        account=sf_config['account'],
        warehouse=sf_config['warehouse'],
        database=sf_config['database'],
        schema=sf_config['schema']
    )

    query = f"SELECT * FROM {table_name}"
    # If you have primary key columns, you can also fetch specific columns or add filtering.
    # Add where clause if needed.
    data = pd.read_sql(query, conn)
    conn.close()

    return data

def get_db2_data(db2_config, table_name, pk_columns):
    """Fetch data from DB2."""
    conn = ibm_db.connect(
        f"DATABASE={db2_config['database']};HOSTNAME={db2_config['hostname']};PORT={db2_config['port']};PROTOCOL=TCPIP;UID={db2_config['user']};PWD={db2_config['password']}",
        "", ""
    )

    query = f"SELECT * FROM {table_name}"
    stmt = ibm_db.exec_immediate(conn, query)
    result = ibm_db.fetch_assoc(stmt)

    db2_data = []
    while result:
        db2_data.append(result)
        result = ibm_db.fetch_assoc(stmt)
    
    db2_df = pd.DataFrame(db2_data)
    ibm_db.close(conn)
    
    return db2_df

def compare_data(sf_data, db2_data, pk_columns):
    """Compare data between Snowflake and DB2 tables."""
    result = {}
    result['total_records_sf'] = len(sf_data)
    result['total_records_db2'] = len(db2_data)
    
    # Compare columns
    sf_columns = set(sf_data.columns)
    db2_columns = set(db2_data.columns)
    
    result['columns_match'] = sf_columns == db2_columns
    result['sf_columns'] = list(sf_columns)
    result['db2_columns'] = list(db2_columns)
    
    # Ensure the primary key columns are set as indices
    sf_data.set_index(pk_columns, inplace=True)
    db2_data.set_index(pk_columns, inplace=True)
    
    # Compare rows based on primary key
    matched = sf_data.merge(db2_data, how='inner', left_index=True, right_index=True)
    mismatched = sf_data.merge(db2_data, how='outer', left_index=True, right_index=True, indicator=True)
    mismatched = mismatched[mismatched['_merge'] != 'both']
    
    result['matched_records'] = matched
    result['mismatched_records'] = mismatched
    
    return result

def write_output(result, output_file):
    """Write comparison results to a text file."""
    with open(output_file, 'w') as f:
        f.write(f"Total Records in Snowflake: {result['total_records_sf']}\n")
        f.write(f"Total Records in DB2: {result['total_records_db2']}\n\n")
        
        f.write("Columns Match: {}\n".format(result['columns_match']))
        f.write("Snowflake Columns: {}\n".format(result['sf_columns']))
        f.write("DB2 Columns: {}\n\n".format(result['db2_columns']))
        
        f.write(f"\nMatched Records Count: {len(result['matched_records'])}\n")
        f.write(result['matched_records'].to_string(index=False))
        
        f.write(f"\nMismatched Records Count: {len(result['mismatched_records'])}\n")
        f.write(result['mismatched_records'].to_string(index=False))
        
def main(sf_config, db2_config, table_name, pk_columns, output_file):
    # Fetch data from Snowflake and DB2
    sf_data = get_snowflake_data(sf_config, table_name, pk_columns)
    db2_data = get_db2_data(db2_config, table_name, pk_columns)
    
    # Compare data
    result = compare_data(sf_data, db2_data, pk_columns)
    
    # Write results to output file
    write_output(result, output_file)
    
    print(f"Comparison completed. Results are saved in {output_file}")

# Example Configuration (update with your actual config)
sf_config = {
    'user': 'your_sf_user',
    'password': 'your_sf_password',
    'account': 'your_sf_account',
    'warehouse': 'your_sf_warehouse',
    'database': 'your_sf_database',
    'schema': 'your_sf_schema'
}

db2_config = {
    'user': 'your_db2_user',
    'password': 'your_db2_password',
    'hostname': 'your_db2_hostname',
    'port': 'your_db2_port',
    'database': 'your_db2_database'
}

# Example: Specify the table name and primary key columns
table_name = 'your_table_name'
pk_columns = ['your_primary_key_column']  # List of primary key column names

# Specify output file path
output_file = 'comparison_results.txt'

# Run the comparison
main(sf_config, db2_config, table_name, pk_columns, output_file)
