import time
import psycopg2
import argparse
import csv

# Database credentials and table info
Database_Name = "postgres"
Database_User = "postgres"
Database_Password = "postgres"
Table_Name = 'census_data'
File_Name = "AL2015_1.csv"
Should_Create_Table = False

def setup_environment():
    # Parse command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--datafile", required=True)
    parser.add_argument("-c", "--createtable", action="store_true")
    args = parser.parse_args()

    global File_Name
    File_Name = args.datafile
    global Should_Create_Table
    Should_Create_Table = args.createtable

def connect_to_database():
    # Establish a database connection
    connection = psycopg2.connect(
        host="localhost",
        database=Database_Name,
        user=Database_User,
        password=Database_Password,
    )
    connection.autocommit = True
    return connection

def setup_database_table(connection):
    # Set up database table structure
    with connection.cursor() as cursor:
        cursor.execute(f"""
            DROP TABLE IF EXISTS {Table_Name};
            CREATE TABLE {Table_Name} (
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
        print(f"Table {Table_Name} has been successfully created.")

def import_data(connection, filepath):
    # Import data into the database using COPY
    with connection.cursor() as cursor, open(filepath, 'r') as file:
        start_time = time.perf_counter()
        next(file)  # Skip the header
        cursor.copy_from(file, Table_Name, sep=',', null='')
        elapsed_time = time.perf_counter() - start_time
        print("Data has been loaded using COPY.")
        print(f'Loading completed. Time taken: {elapsed_time:.4f} seconds')

def verify_table_existence(connection):
    # Verify if the table exists in the database
    with connection.cursor() as cursor:
        cursor.execute("SELECT EXISTS(SELECT 1 FROM pg_tables WHERE tablename = 'census_data');")
        exists = cursor.fetchone()[0]
        if exists:
            print("The 'census_data' table exists in the database.")
        else:
            print("The 'census_data' table does not exist in the database.")

def main():
    setup_environment()
    connection = connect_to_database()
    if Should_Create_Table:
        setup_database_table(connection)
    import_data(connection, File_Name)

if __name__ == "__main__":
    main()
