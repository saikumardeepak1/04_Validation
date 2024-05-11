import time
import psycopg2
import argparse
import csv

database_name = "postgres"
database_user = "postgres"
database_password = "postgres"
table_name = 'CensusData'
file_name = "AL2015_1.csv"  # The data file to be loaded
create_table_flag = False  # Should the DB table be created anew?


def format_row_values(row):
    for key in row:
        if not row[key]:
            row[key] = 0  # Default values for NULLs
        row['County'] = row['County'].replace('\'', '')  # Remove quotes from literals

    values_string = f"""
	   {row['CensusTract']},            
	   '{row['State']}',                
	   '{row['County']}',               
	   {row['TotalPop']},               
	   {row['Men']},                    
	   {row['Women']},                  
	   {row['Hispanic']},               
	   {row['White']},                  
	   {row['Black']},                  
	   {row['Native']},                 
	   {row['Asian']},                  
	   {row['Pacific']},                
	   {row['Citizen']},                
	   {row['Income']},                 
	   {row['IncomeErr']},              
	   {row['IncomePerCap']},           
	   {row['IncomePerCapErr']},        
	   {row['Poverty']},                
	   {row['ChildPoverty']},           
	   {row['Professional']},           
	   {row['Service']},                
	   {row['Office']},                 
	   {row['Construction']},          
	   {row['Production']},             
	   {row['Drive']},                  
	   {row['Carpool']},                
	   {row['Transit']},                
	   {row['Walk']},                   
	   {row['OtherTransp']},            
	   {row['WorkAtHome']},             
	   {row['MeanCommute']},            
	   {row['Employed']},               
	   {row['PrivateWork']},            
	   {row['PublicWork']},             
	   {row['SelfEmployed']},           
	   {row['FamilyWork']},             
	   {row['Unemployment']}            
	"""

    return values_string


def setup():
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--datafile", required=True)
    parser.add_argument("-c", "--createtable", action="store_true")
    args = parser.parse_args()

    global file_name
    file_name = args.datafile
    global create_table_flag
    create_table_flag = args.createtable


def fetch_data(filepath):
    print(f"Fetching data from file: {filepath}")
    with open(filepath, mode="r") as file:
        data_reader = csv.DictReader(file)

        data_list = []
        for data in data_reader:
            data_list.append(data)

    return data_list


def create_sql_commands(data_list):
    sql_commands = []
    for data in data_list:
        value_string = format_row_values(data)
        command = f"INSERT INTO {table_name} VALUES ({value_string});"
        sql_commands.append(command)
    return sql_commands


def connect_db():
    connection = psycopg2.connect(
        host="localhost",
        database=database_name,
        user=database_user,
        password=database_password,
    )
    connection.autocommit = True
    return connection


def setup_table(connection):
    with connection.cursor() as cursor:
        cursor.execute(f"""
			DROP TABLE IF EXISTS {table_name};
			CREATE TABLE {table_name} (
				CensusTract         NUMERIC,
				State               TEXT,
				County              TEXT,
				TotalPop            INTEGER,
				Men                 INTEGER,
				Women               INTEGER,
				Hispanic            DECIMAL,
				White               DECIMAL,
				Black               DECIMAL,
				Native              DECIMAL,
				Asian               DECIMAL,
				Pacific             DECIMAL,
				Citizen             DECIMAL,
				Income              DECIMAL,
				IncomeErr           DECIMAL,
				IncomePerCap        DECIMAL,
				IncomePerCapErr     DECIMAL,
				Poverty             DECIMAL,
				ChildPoverty        DECIMAL,
				Professional        DECIMAL,
				Service             DECIMAL,
				Office              DECIMAL,
				Construction        DECIMAL,
				Production          DECIMAL,
				Drive               DECIMAL,
				Carpool             DECIMAL,
				Transit             DECIMAL,
				Walk                DECIMAL,
				OtherTransp         DECIMAL,
				WorkAtHome          DECIMAL,
				MeanCommute         DECIMAL,
				Employed            INTEGER,
				PrivateWork         DECIMAL,
				PublicWork          DECIMAL,
				SelfEmployed        DECIMAL,
				FamilyWork          DECIMAL,
				Unemployment        DECIMAL
			);	
		""")

        print(f"Table {table_name} created successfully.")


def load_data(connection, sql_commands):
    with connection.cursor() as cursor:
        print(f"Loading {len(sql_commands)} rows")
        start = time.perf_counter()

        for command in sql_commands:
            cursor.execute(command)

        elapsed = time.perf_counter() - start
        print(f'Data loaded. Time taken: {elapsed:0.4} seconds')


def main():
    setup()
    connection = connect_db()
    data_list = fetch_data(file_name)
    sql_commands = create_sql_commands(data_list)

    if create_table_flag:
        setup_table(connection)

    load_data(connection, sql_commands)


if __name__ == "__main__":
    main()
