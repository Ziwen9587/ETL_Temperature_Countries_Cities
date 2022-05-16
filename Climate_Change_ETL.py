import json
import time
import traceback

import pandas as pd

from geoalchemy2 import Geometry

from sqlalchemy import *
from sqlalchemy import schema



"""
*******************************************************************************************************
***************************THE GENERAL PROCEDURE OF THE ETL PROCESS************************************
*******************************************************************************************************

1. Database Preparation & Table Creation:
        (1). Prepare database connection: Fill the value of keys in config.json file.
        (3). Connect database & create relational tables: Call method create_table().

2. ETL of Source Data:
        (1). Implement ETL process for CSV files(temperature for cities, countries): Call method ETL_process().

3. Build a Joined Table for Two Source Tables:
        (1). Create the blank joined table: Call method create_table().
        (2). Generate the joined data in the joined table: Call method update_jointable().

*******************************************************************************************************
*******************************************************************************************************
*******************************************************************************************************
"""

class DbConnectSqlAlchemy:
    """
    The class DbConnectSqlAlchemy is used to create an object to connect database system. the underlying implementation
    of the connection is undertaken by Python library SQLAlchemy.
    Note: the json file named "config.json" is required since it contains all the parameters for database connection.

    Preliminaries:
        files required: config.json.
        files path: the config.json should be in the same directory with this Python file.
    """

    def __init__(self):
        try:
            with open("./config.json") as jsonfile:
                config = json.load(jsonfile)

                database = config["database"]
                user = config["user"]
                password = config["password"]
                host = config["host"]
                port = config["port"]
                dialect = "postgresql"  # dialect is DBMS type, such as mysql, sqlite, postgresql, etc.
                url = f"{dialect}://{user}:{password}@{host}:{port}/{database}"

                # Use sqlalchemy engine configuration to connect the database system.
                self.engine = create_engine(url, pool_size=5, isolation_level="AUTOCOMMIT")
                self.conn = self.engine.connect()
                self.raw_conn = self.engine.raw_connection()  # Raw connection help invoke the connection from DBAPI
                print("Connection to database successfully... \n")

        except Exception as e:
            print(f"Connection to database failed: {e} \n")
        self.start_time = time.time()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.conn.close()
        self.raw_conn.close()
        print(
            f"Released database connection: held for {time.time() - self.start_time} seconds \n")


def trigger_generated_geometry_column(db_obj, table_name, column_longitude="longitude", column_latitude="latitude",
                                      geometry_column="geometry"):
    """
    A helper function for add_constraint().
    Create both a trigger function and a trigger for the specified table. The trigger function and the tigger are used
    to generate longitude&latitude geometry points in a geometry column of a relational table in the database system.

    Preliminaries:
        The relational table must already have the geometry column.

    Parameters:
        db_obj: An instance of DbConnectSqlAlchemy() class.
        table_name: The name of an existing relational table in the database system. Data type: String.
        column_longitude: The name of longitude column. Default="longitude". Data type: String.
        column_latitude: The name of latitude column. Default="latitude". Data type: String.
        geometry_column: The name of longitude&latitude geometry column. Default="geometry". Data type: String.

    Return:
        Create a trigger function and a trigger for the specified relational table in the database
    """

    try:
        query_trigger_function = f"""
                                    CREATE OR REPLACE FUNCTION {table_name}_generated_geometry_point()
                                      RETURNS TRIGGER
                                      LANGUAGE PLPGSQL
                                      AS
                                    $$
                                    BEGIN
                                        NEW.{geometry_column} := ST_Transform( ST_SetSRID( ST_MakePoint(NEW.{column_longitude}, NEW.{column_latitude}, 0), 4326), 3347); 
                                        RETURN NEW;
                                    END;
                                    $$
                                """
        db_obj.conn.execute(query_trigger_function)
        query_trigger = f"""
                                CREATE TRIGGER TR_{table_name}_generated_geometry_point
                                BEFORE INSERT OR UPDATE
                                ON {table_name}
                                FOR EACH ROW
                                EXECUTE PROCEDURE {table_name}_generated_geometry_point()
                            """
        db_obj.conn.execute(query_trigger)
        print(f"SUCCESS: Created trigger function for {table_name}.geometry column.")
    except Exception as e:
        print(f"Fail: Fail to create trigger function for {table_name}.geometry column by {e} \n")
        print(traceback.format_exc())



def add_constraint(db_obj, schema_name, table_name):
    """
    A helper function for create_table().
    Add constraints and triggers to a specified and existing relational table in the database. The constraints are
    specified under the key named "unique" in the json file named "climate_table.json". The triggers
    are explained in the helper functions: trigger_generated_geometry_column.

    Preliminaries:
        files required: climate_table.json
        files path: The config.json should be in the same directory with this Python file.
        database requirements: The relational table must already exist.
        helper functions: trigger_generated_geometry_column().

    Parameters:
        db_obj: An instance of DbConnectSqlAlchemy() class.
        schema_name: The name of the schema. Data type: String.
        table_name: The name of the existing table. Data type: String.

    Return:
        add constraints and triggers to the specified relational table in the database.
    """

    with open('./climate_table.json', 'r') as jsonfile:
        config_db = json.load(jsonfile)

        for db_table in config_db["database"]:
            # STEP.1: Locate the targeted table in the climate_table.json file's database
            if (db_table["name"] == table_name):

                # STEP.2: Generate a string named pk_string which contains all the columns with unique constraint.
                if (table_name.lower().find("link") == -1):
                    pk_json_list = db_table["unique"]
                    pk_string = ""
                    for item in pk_json_list:
                        pk_string += item["name"] + ","
                    pk_string = pk_string.strip(pk_string[-1])


                # STEP.3: Based on the pk_string, add unique constaint to the relational table
                if (table_name.find("link") == -1):     # Case 1: Not a joined table
                    if (table_name.lower().find("city") != -1 ):           # Case 1.1:
                        trigger_generated_geometry_column(db_obj, table_name)
                    db_obj.conn.execute(text(
                        f"ALTER TABLE {schema_name}.{table_name} ADD CONSTRAINT unique_{table_name} UNIQUE ({pk_string})"))

def create_table(table_name, schema_name="public"):
    """
    Create a table in the database system. The table names should match a table name in the climate_table.json file.

    Preliminary:
        file required: climate_table.json.
        files path: the config_db.json should be in the same directory with this Python file.
        helper functions: add_constraint().

    Parameters:
        table_name: Name of the target table to be created. Data type:String. Note: the table_name must match a
                    table name in climate_table.json file.
        schema_name: Schema name of the database(default is "public"). Data type::String. Note: the schema_name must
                    match a schema name in the database system.

    Return:
        Create a relational table in the database system.
    """

    try:
        with open('./climate_table.json', 'r') as jsonfile:
            config_db = json.load(jsonfile)
            flag_table_exist = False

            for db_table in config_db["database"]:
                if (db_table["name"] == table_name):
                    columns = db_table["columns"]
                    flag_table_exist = True

                    # Call DbConnectSqlAlchemy() to make connection to the database system
                    with DbConnectSqlAlchemy() as db:
                        metadata_obj = schema.MetaData(bind=db.engine)  # Create metadata object as the table collection
                        table = Table(table_name, metadata_obj, schema=schema_name)  # Create a SQLAlchemy Table object

                        for column in columns:  # Iterate columns to append SQLAlchemy Column object to the Table object
                            if column["dtype"] == "float":
                                table.append_column(Column(column["name"], Float))
                            elif column["dtype"] == "date":
                                table.append_column(Column(column["name"], Date))
                            elif column["dtype"] == "text":
                                table.append_column(Column(column["name"], Text))
                            elif column["dtype"] == "boolean":
                                table.append_column(Column(column["name"], Boolean))
                            elif column["dtype"] == "geometry":
                                table.append_column(
                                    Column(column["name"],
                                           Geometry(geometry_type='GEOMETRYZ', srid=3347, dimension=3,
                                                    spatial_index=True)))
                        table.create()                              # Create the relational table in the database system
                        add_constraint(db, schema_name, table_name)     # Add constraint to the relational table
                    print(f"SUCCESS: Create table {schema_name}.{table_name}\n")

            if (flag_table_exist == False):
                print(f"Fail: The table {table_name} does not exist in the climate_table.json file")
    except Exception as e:
        print(f"FAIL: Fail to create table by {e} \n")
        print(traceback.format_exc())




def clean_latitude_longtitude(value):
    """
        Clean the latitude and longitude value. Removes the character and convert it to corresponding
        numeric positive or negative value in string type.

        Parameters:
            value: a mixture of decimal and upper-case character including "E", "W", "N", and "S".

        Return:
            the numeric representation of latitude or longitude in string type
        """
    if value.find("W") != -1 or value.find("S") != -1:
        value = "-"+value
    value = value.rstrip(value[-1])
    return value


def ETL_process(table_name, file_path, schema_name="public"):
    """
    An ETL process for source data. Extract data from CSV file. Transform data including clean the longitude and
    latitude, and deduplicate records. Load data into the database system.

    Preliminaries:
        helper function: clean_latitude_longtitude()

    Parameters:
        table_name: Name of an existing relational table in database.
        file_path: File path of the source data.
        schema_name: Schema name of the database. Default="public".

    Return:
        Automate ETL process
    """

    shp_start_time = time.time()
    try:
        with DbConnectSqlAlchemy() as db:
            df=pd.read_csv(file_path)

            # STEP.1 Rename the columns in Pandas dataframe to lower case.
            for column_name in df.columns:
                df.rename(columns={column_name: column_name.lower()}, inplace=True)

            # STEP.2 Clean latitude and longitude
            lowerstring_path = file_path.lower()
            if lowerstring_path.find("city") != -1:
                for column_name_city in df.columns:
                    if column_name_city.find('temperature') != -1:
                        df.rename(columns={column_name_city: "city_"+column_name_city}, inplace=True)
                df.longitude = df.longitude.apply(clean_latitude_longtitude)
                df.latitude = df.latitude.apply(clean_latitude_longtitude)
            elif lowerstring_path.find("country") != -1:
                for column_name_country in df.columns:
                    if column_name_country.find('temperature') != -1:
                        df.rename(columns={column_name_country: "country_" + column_name_country}, inplace=True)

            # STEP.3 Drop duplicated rows
            df.drop_duplicates(keep='first', inplace=True, )

            # STEP.3 Load data into the database
            df.to_sql(table_name,con=db.engine,schema=schema_name, if_exists="append", index=False)


        print(f"SUCCESS: processed ETL for the {file_path} in {schema_name}.{table_name}")
        cost_time = time.time() - shp_start_time
        print(f"Time cost for automate ETL process: {cost_time} seconds \n")
    except Exception as e:
        print(f"FAIL: Error existing while automate ETL process: {e} \n")
        print(traceback.format_exc())


def update_jointable(link_table_name="link_temperature_city_country", schema_name="public"):
    """
    A transform phase and a load phase of ETL. Generate the natural join between source tables
    (global_and_temperatures_by_major_city, global_and_temperatures_by_country), and load the joined data into the
    database system. The natural join is based on columns date and country of each source tables.

    Preliminary:
        database required: The joined relational table must already exist in database.

    Parameters:
        link_table_name: Name of the existing joined table. Default="link_temperature_city_country". Data type: string.
        schema_name: Schema name of the database. Default="public". Data type: string.

    Return:
        Produces the joined data contains both temperature info of country and temperature info of its corresponding
        major cities.
    """

    update_start_time = time.time()
    try:
        with open('./climate_table.json', 'r') as jsonfile:
            config_db = json.load(jsonfile)
            flag_table_exist = False

            for db_table in config_db["database"]:
                # STEP.1 Locate the table in the climate_table.json file
                if (db_table["name"] == link_table_name):
                    combination = db_table["combination"]
                    flag_table_exist = True

                    # STEP.2 Get the table names and corresponding referenced columns.
                    table1_name = combination[0]["table1"]
                    table1_column_lst = combination[0]["table1_columns"]
                    table2_name = combination[1]["table2"]
                    table2_column_lst = combination[1]["table2_columns"]

                    # STEP.3 Process data transformation and loading.
                    with DbConnectSqlAlchemy() as db:
                        # Use SQLAlchemy core to map from the relational tables into Python objects here.
                        metadata_obj = schema.MetaData(bind=db.engine)
                        link_table = Table(link_table_name, metadata_obj, schema=schema_name, autoload_with=db.engine)
                        table1 = Table(table1_name, metadata_obj, schema=schema_name, autoload_with=db.engine)
                        table2 = Table(table2_name, metadata_obj, schema=schema_name, autoload_with=db.engine)

                        # Generate a select list of sqlalchemy.sql.expression.ColumnElement from both source tables
                        lst = []
                        for column1 in table1_column_lst:
                            lst.append(table1.columns[column1])
                        for column2 in table2_column_lst:
                            lst.append(table2.columns[column2])

                        # SQLAlchemy syntax: Used the select list to select specified columns from the joined table
                        selection = select(lst).select_from(table1).join(table2, (table1.c.dt==table2.c.dt)&(table1.c.country==table2.c.country) )

                        # Insert the joined data from the selection above into a relational table in the database system
                        insertion = link_table.insert().from_select(table1_column_lst + table2_column_lst, selection)

                        # Implement the insertion
                        db.conn.execute(insertion)

                    print(f"SUCCESS: Updated data in the joined table {link_table_name}")
                    cost_time = time.time() - update_start_time
                    print(f"Time cost is: {cost_time} seconds \n")
            if (flag_table_exist == False):
                print(f"Fail: The joined table {link_table_name} does not exist in the climate_table.json file")
    except Exception as e:
        print(f"FAIL: Error exists while updating the joined table: {e} \n")
        print(traceback.format_exc())





# ETL_process("climate_change","Climate_change/GlobalLandTemperaturesByCountry.csv")
# ETL_process("test","Climate_change/testfile.csv")

# create_table("global_and_temperatures_by_major_city")
# ETL_process("global_and_temperatures_by_major_city","Climate_change/GlobalLandTemperaturesByMajorCity.csv")

# create_table("global_and_temperatures_by_country")
# ETL_process("global_and_temperatures_by_country","Climate_change/GlobalLandTemperaturesByCountry.csv")

# create_table("link_temperature_city_country")
# update_jointable(link_table_name="link_temperature_city_country")