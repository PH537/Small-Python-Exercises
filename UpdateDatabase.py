pk_repository = {
    "SalesOrderHeader": "SalesOrderID",
    "WorkOrder":        "WorkOrderID", 
    "BusinessEntity":   "BusinessEntityID", 
    "EmailAddress":    ["BusinessEntityID", "EmailAddressID"], 
    "PersonPhone":     ["BusinessEntityID", "PhoneNumber", "PhoneNumberTypeID"], 
    "Person":           "BusinessEntityID", 
    "Customer":         "CustomerID"
    }
keys_example = {
    "SalesOrderID": 75123,
    "WorkOrderID": 72591, 
    "BusinessEntityID": 20777,
    "EmailAddressID": 19972,
    "PhoneNumber": "'1 (11) 500 555-0126'", 
    "PhoneNumberTypeID": 2,
    "CustomerID": 30118
    }

# Creates a list of dictionaries, each representing an update in the 
# source database so that we can test the program.
def create_fictional_update_list():
    # Exchange the fields of the primary key from pk_repository for a suitable value from keys_example and put in a new list called
    # list_tables_and_keys for easier reference later.
    list_tables_and_values = []
    for table_name, key in pk_repository.items():
      if type(key) == str:
        list_tables_and_values.append((table_name, keys_example[key]))
      elif type(key) == list:
        key_list = []
        for field in key:
          key_list.append(keys_example[field])
        list_tables_and_values.append((table_name, key_list))
      else:
        raise TypeError(f"key is of type {type(key)}! Must be either a string or list")
# Use the list of tables and key values to produce update dicts with keys table_name, update type and pk value. 
# Makes three updates per table, one for each update_type (delete, insert, update). 
# Function returns a list of dicts.      
    fict_update_list = []
    for table_name, value in list_tables_and_values:
        for update_type in ("Delete","Insert","Update"):
            new_dict = {"table_name": table_name, "update_type": update_type, "prim_key_value": value}
            fict_update_list.append(new_dict)
    return fict_update_list

# Returns a string that serves as the WHERE-clause of an sql command. table_name is the table being targeted, prim_key_value is the value of 
# the fields that are selection criteria in the sql query. The fields are taken from pk_repository.
def where_clause(table_name, prim_key_value):
    where = "WHERE "
    value_type = type(prim_key_value)
    if (value_type == int) or (value_type == str):
        where += f"{pk_repository[table_name]} = {prim_key_value}"
    elif type(prim_key_value) == list:
        fields_and_values = list(zip(pk_repository[table_name], prim_key_value))
        length = len(fields_and_values)
        for i in range(length):
            condition = f"""
            {fields_and_values[i][0]} = {fields_and_values[i][1]}
            """
            where += condition
            if i < (length - 1):
                where += " AND "
    else:
        raise TypeError(f"Prim_key_value is of type {type(prim_key_value)}!")
    return where

# Vraag de gewijzigde row op uit de server met een query: SELECT * FROM table_name WHERE column = key (AND...);
# Voer deze wijziging meteen uit in databricks.
# Al is het misschien efficienter om eerst alle queries te doen op de ene database, en daarna alle edits op de andere database.
def copy_data_from_source():
  for i in range(len(database_updates)):
    if i > 2: break      # just debugging one table for now
    table_name    = database_updates[i]["table_name"]        
    update_type   = database_updates[i]["update_type"]          
    prim_key_value   = database_updates[i]["prim_key_value"]  
    where         = where_clause(table_name, prim_key_value)
    target_table =  "hive_metastore.testdatabase." + table_name
    source_table = find_source_table_name(table_name)
    
    # Determine what kind of change took place, and perform the same operation on the table in azure
    if  update_type == "Update":
            updated_record = do_query(source_table, where)             #  haal record uit brontabel
            update_command = do_update(updated_record, update_type, target_table, where)
            print(update_command)

    elif update_type == "Insert":
        updated_record = do_query(source_table, where)
        insert_command = do_update(updated_record, update_type, target_table, where)
        print(insert_command)
        #select_query = f"SELECT * FROM {source_table} {where};"
        #inserted_record = spark.sql(select_query)                                       
        #insert_command = f"INSERT INTO {target_table} VALUES ;"      
        #result = spark.sql({insert_command})                                            # voer insert uit

    elif update_type == "Delete":
        delete_command = f"DELETE FROM {target_table} {where};"             # delete-type heeft geen query nodig, meteen deleten
        #result = spark.sql({delete_command})
        print(delete_command)

# Restores the original schema name in the source table name.
def find_source_table_name(table_name):
    if table_name in ["BusinessEntity","EmailAddress","PersonPhone","Person"]:
        return f"Person.{table_name}"
    elif table_name in ["SalesOrderHeader", "Customer"]:
        return f"Sales.{table_name}"
    elif table_name == "WorkOrder":
      return f"Production.{table_name}"

# Perform query on SQL Server database
def do_query(source_table, where):
    df = spark.read \
    .format("jdbc") \
    .option("url", jdbcUrl) \
    .option("dbtable", source_table) \
    .option("user", jdbcUsername) \
    .option("password", jdbcPassword) \
    .load()
    df.createOrReplaceTempView("source_view")
    return spark.sql(f"SELECT * FROM source_view {where};")

# Perform update on databricks database
def do_update(updated_record, update_type, target_table, where):
    columns, values = convert_data_frame(updated_record, 0)
    # Construct the SQL command
    if update_type == "Update":
        set_list = []
        for field, value in zip(columns, values):
            new_pair = f"{field} = '{value}'"
            set_list.append(new_pair)
        set_str = ", ".join(set_list) 
        update_command = f"UPDATE {target_table} SET {set_str} {where};"
        return update_command
    elif update_type == "Insert":
        columns_str = ", ".join(columns)
        values_str = ", ".join([f"'{value}'" if isinstance(value, str) else str(value) for value in values])
        insert_command = f"INSERT INTO {target_table} ({columns_str}) VALUES ({values_str});"
        return insert_command

# Convert the result of a query to a list of columns and values
# print parameter is boolean, prints out columns with their value and data type
def convert_data_frame(selection, print = 0):
        pandas_df = selection.toPandas()
        columns = pandas_df.columns.tolist()
        values = pandas_df.iloc[0].tolist()
        for value in values:
            if value == NaN:
                value = NULL
        # print column, value, data type
        if print == True:
            data_types = [type(value) for value in values] 
            list_types = [f"{column} = {value} ({data_type})" for column, value, data_type in zip(columns, values, data_types)]
            print(list_types)
        return columns, values

# inloggen en spark sessie starten
# Import necessary libraries
from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("SQLServertoDatabricks").getOrCreate()

# Connection parameters, globals
jdbcHostname = "dqc-ontwikkel.database.windows.net"
jdbcPort = 1433
jdbcDatabase = "AdventureWorks"
jdbcUsername = "topp-it_peter"
jdbcPassword = "wyR88zbbqZA2oHrLuTnW"

# JDBC URL, global
jdbcUrl = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};database={jdbcDatabase}"

database_updates = create_fictional_update_list()
copy_data_from_source()   


# inspecting data

command = """
UPDATE hive_metastore.testdatabase.SalesOrderHeader 
SET SalesOrderID = '75123', RevisionNumber = '8', OrderDate = '2014-06-30 00:00:00', DueDate = '2014-07-12 00:00:00', ShipDate = '2014-07-07 00:00:00', 
Status = '5', OnlineOrderFlag = 'True', SalesOrderNumber = 'SO75123', PurchaseOrderNumber = 'None', AccountNumber = '10-4030-018759', CustomerID = '18759', 
SalesPersonID = NULL, TerritoryID = '6', BillToAddressID = '14024', ShipToAddressID = '14024', ShipMethodID = '1', CreditCardID = '10084', 
CreditCardApprovalCode = '230370Vi51970', CurrencyRateID = NULL, SubTotal = '189.9700', TaxAmt = '15.1976', Freight = '4.7493', TotalDue = '209.9169', 
Comment = 'None', rowguid = 'D54752FF-2B54-4BE5-95EA-3B72289C059F', ModifiedDate = '2014-07-07 00:00:00' WHERE SalesOrderID = 75123;
"""

#result = spark.sql(command)
#print(list(result)[0][1])
#display(result)
print(convert_data_frame(spark.sql("""
                SELECT * FROM hive_metastore.testdatabase.SalesOrderHeader WHERE SalesOrderID = 75123
                """
                )))
