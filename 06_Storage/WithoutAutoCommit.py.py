import time
import psycopg2
import argparse
import csv

# Configuration for database connection
database_name = "postgres"
database_user = "postgres"
database_password = "postgres"
table_name = 'CensusData'
data_file = "AL2015_1.csv"  # Data file for loading
recreate_table = False  # Flag to check if table needs to be recreated

def prepare_values(row):
    # Clean and format row data for SQL insertion
    for key in row:
        if not row[key]:
            row[key] = 0  # Default for missing values
        row['County'] = row['County'].replace('\'', '')  # Remove apostrophes

    sql_values = f"""
        {row['CensusTract']},            -- CensusTract
        '{row['State']}',                -- State
        '{row['County']}',               -- County
        {row['TotalPop']},               -- TotalPop
        {row['Men']},                    -- Men
        {row['Women']},                  -- Women
        {row['Hispanic']},               -- Hispanic
        {row['White']},                  -- White
        {row['Black']},                  -- Black
        {row['Native']},                 -- Native
        {row['Asian']},                  -- Asian
        {row['Pacific']},                -- Pacific
        {row['Citizen']},                -- Citizen
        {row['Income']},                 -- Income
        {row['IncomeErr']},              -- IncomeErr
        {row['IncomePerCap']},           -- IncomePerCap
        {row['IncomePerCapErr']},        -- IncomePerCapErr
        {row['Poverty']},                -- Poverty
        {row['ChildPoverty']},           -- ChildPoverty
        {row['Professional']},           -- Professional
        {row['Service']},                -- Service
        {row['Office']},                 -- Office
        {row['Construction']},           -- Construction
        {row['Production']},             -- Production
        {row['Drive']},                  -- Drive
        {row['Carpool']},                -- Carpool
        {row['Transit']},                -- Transit
        {row['Walk']},                   -- Walk
        {row['OtherTransp']},            -- OtherTransp
        {row['WorkAtHome']},             -- WorkAtHome
        {row['MeanCommute']},            -- MeanCommute
        {row['Employed']},               -- Employed
        {row['PrivateWork']},            -- PrivateWork
        {row['PublicWork']},             -- PublicWork
        {row['SelfEmployed']},           -- SelfEmployed
        {row['FamilyWork']},             -- FamilyWork
        {row['Unemployment']}            -- Unemployment
    """

    return sql_values

def configure():
    # Set up command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--datafile", required=True)
    parser.add_argument("-c", "--createtable", action="store_true")
    args = parser.parse_args()

    global data_file
    data_file = args.datafile
    global recreate_table
    recreate_table = args.createtable

def load_data_from_file(filepath):
    # Read data from CSV file
    print(f"Loading data from: {filepath}")
    with open(filepath, 'r') as file:
        reader = csv.DictReader(file)
        data = [row for row in reader]
    return data

def generate_insert_commands(data):
    # Generate SQL commands for data insertion
    commands = []
    for item in data:
        values = prepare_values(item)
        command = f"INSERT INTO {table_name} VALUES ({values});"
        commands.append(command)
    return commands

def connect_to_database():
    # Establish database connection
    return psycopg2.connect(
        host="localhost",
        database=database_name,
        user=database_user,
        password=database_password,
        autocommit=False
    )

def create_database_table(connection):
    # Create table if needed
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
        print(f"Table {table_name} has been created.")
        connection.commit()

def main():
    configure()
    connection = connect_to_database()
    data = load_data_from_file(data_file)
    sql_commands = generate_insert_commands(data)

    if recreate_table:
        create_database_table(connection)

    # Execute data loading
    with connection.cursor() as cursor:
        print(f"Inserting {len(sql_commands)} records into the database.")
        start_time = time.perf_counter()
        for command in sql_commands:
            cursor.execute(command)
        connection.commit()
        elapsed_time = time.perf_counter() - start_time
        print(f'Finished loading. Time elapsed: {elapsed_time:.4f} seconds.')

if __name__ == "__main__":
    main()
