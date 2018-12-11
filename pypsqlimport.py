# pypsqlimport.py creates table based on arguments at launch and a given delimited text file to a database
#
# import sys module for access to sys.argv
import sys
# import getpass to get a password
import getpass
# import PostgreSQL library
import psycopg2
# defining a function for connecting to the db.
def db_connect(db, u, pw, h, pn):
    
    # try/except/else attempt connection to database, generate a human readable error message if attempt is unsuccessful.
    try:
        con = psycopg2.connect(database = database, user = user, password = password, host = host, port = port, sslmode = "disable")

    except psycopg2.Error as err:
        print("An error was generated!")
        print(err)
        print("Terminating; Thank you for using this script")
        exit()
        
    else:
        print("Connection to database was successful!")
    # end of try/except/else for connection

    return con
# end db_connect()
    
# declare variables necessary for database connection
database = " "
user = " "  
password = " " 
host = " " 
port = " "

# declare variables for file operations
filepath = " "

# declare variables for PSQL operations
schema_exists = False
schema = " "
table_name = " "

# store sys.argv in args[] for simplicity
args = sys.argv
# define index for last arg
last_arg_index = len(args)

# check if any arguments were provided. args have been provided if len() is greater than 1 (scripts filename counts as an argument to python)
if len(args) > 1 : 
    # check if user has requested help
    if args[1].rstrip() == "-help":
        # print help text
        print("provide arguments with flags")
        # exit script
        exit()
    # end if
    
    # go through args to look for key-value pairs. arg_index is used to iterate in the loop
    arg_index = 1
    while arg_index < len(args):
        
        # break for loop if current index is equal to last_arg_index, as this would mean no further key-value pairs can be determined 
        if arg_index == last_arg_index:
            print("Insufficient values provided, you will be prompted for remaining values")
            break
        # end if last_arg_index
        
        # check if arg is equal to -db
        elif args[arg_index].rstrip() == "-db":
            # increment i so the value to the key -db can be extracted from args[]  
            arg_index += 1
            # store database value in database variable
            database = args[arg_index].rstrip() 
        # end elif -db
        
        # check if arg is equal to -u
        elif args[arg_index].rstrip() == "-u":
            # increment i so the value to key -u can be extracted from args[]
            arg_index += 1
            # store user value in user variable
            user = args[arg_index].rstrip()
        # end of -u elif
        
        # check if arg is equal to -pw
        elif args[arg_index].rstrip() == "-pw":
            arg_index += 1
            password = args[arg_index].rstrip()
        # end of -pw elif
        
        # check if arg is equal to -h
        elif args[arg_index].rstrip() == "-h":
            arg_index += 1
            host = args[arg_index].rstrip()
        # end of -u elif
        
        # check if arg is equal to -pn
        elif args[arg_index].rstrip() == "-pn":
            arg_index += 1
            port = args[arg_index].rstrip()
        # end of -pn elif
        
        # check if arg is equal to -fp
        elif args[arg_index].rstrip() == "-fp":
            arg_index += 1
            filepath = args[arg_index]
        else:
             print("Unrecognized key {key}".format(key = args[arg_index]))
             arg_index += 1
        # end if/elif/else structure
        
        # increase arg_index by one to continue loop to next key-value pair
        arg_index += 1
    # end of while loop
# end of if len(args)

# check if necessary db-parameters are set properly, prompt user for new values if necessary
if not (database[0].isalpha() and database.isalnum()):
    while True:
        db_input = input("Please enter database name: ").strip()
        
        if db_input[0].isalpha() and db_input.isalnum():
            database = db_input
            break
        else:
            print("So sorry, we only support databases starting with an alphabetic character, consisting of only letters and numbers, please try again")
            continue
        # end if
    # end while
# end database if
# check if necessary -u parameters are set properly, prompt user for new values if necessary
if not (user[0].isalpha() and user.isalnum()):
    while True:
        u_input = input("Please enter user name: ").strip()
        
        if u_input[0].isalpha() and u_input.isalnum():
            user = u_input
            break
        else:
            print("So sorry, we only support usernames starting with an alphabetic character, consisting of only letters and numbers, please try again")
            continue
        # end if
    # end while
# end user if

# check if -pw values are set properly, prompt user for new values if necessary
if password == " ":
    # check if terminal is tty, example of non-tty: Git Bash
    if sys.stdin.isatty():
        password = getpass.getpass("Please provide a password for user [{user}]: ".format(user = user))
    else:
        print("getpass is not supported by your terminal, using regular input (password will be visible :/)")
        password = input("Please provide a password for user [{user}]: ".format(user = user)).rstrip()
    # end if else isatty()

# check if -h values are set properly, prompt user for new values if necessary
if host.isspace():
    while True:
        h_input = input("Please enter hostname or IP-address: ").strip()
        
        if h_input[0].isnumeric() or h_input[0].isalpha():
            host = h_input
            break
        else:
            print("So sorry, we need a valid address for the host, please try again")
            continue
        # end if/else h_input
    # end while
# end host.isspace()

# check if -pn is set, prompt user for new values if necessary
if not port.isnumeric():
    pn_input = input("Please supply the portnumber for your host [5432]: ").strip()
    
    # set port to 5432 if non-numeric value is set
    if not pn_input.isnumeric():
        port = "5432"
    else:
        port = pn_input
    # end if/else not pn_input.isnumeric()
# end if not port.isnumeric()
connection = db_connect(database, user, password, host, port)
# try/except/else to open file byway of filepath variable, prompt user for values on except
try:
    f = open(filepath)
except FileNotFoundError as err:
    # prompts user to enter filepath/-name for textfile
    while True:
        filepath = input("Please provide a filename: ").strip()
        
        # try/except/else - attempt to open textfile, provide user-friendly error message if it fails
        try:
            f = open(filepath)
        except FileNotFoundError as err:
            print("Terribly sorry, we can't make sense of this filename/path. Please try again.")
            
            # continue loop, user must input filename again
            continue
        else:
            print("File loaded successfully")
            
            # break while loop
            break
        # end of try except fail
    # end of while loop
else:
    print("File loaded successfully")
#end of try/except/else

# list - store records for import into schema.table_name
records = []

# go through textfile line by line
for i in f.readlines() :
    # append each textfile line to records list in as a list using "/ " to delimit values
    records.append(i.split("/ "))

# end of f.readlines() for-loop

# close textfile
f.close()

cursor = connection.cursor()

# create schema or use existing schema
# while loop runs as long as schema_exists value is False
while not schema_exists:
    sch_input = input("Please provide a name for your schema: ").strip()
    
    # check if input matches our criteria
    if sch_input[0].isalpha() and sch_input.isalnum():
        schema = sch_input
        
        # query sql server to check if schema exists, prompt user to create if not
        while True:
            # the PSQL query will return one row if the schema exists
            cursor.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{schema}';".format(schema = schema))
            
            # check for 1 row
            if cursor.rowcount > 0:
                # set schema_exists to True, then break inner while loop, outer while loop will stop because while not schema_exists will return bool False
                schema_exists = True
                break
            # end of if
            
            # if schema does not exist, offer to create it 
            else:
                cresch_input = input("It appears that schema {schema} does not exist, would you like to create it? Y/n: ".format(schema = schema)).strip()
                
                if cresch_input.upper() != "Y":
                    # user does not want to create the schema 
                    print("That's cool, going back to the schema name input phase!")
                    # break exits inner while loop, outer loop begins anew.
                    break
                # end cresch_input == "Y" if
                
                # attempt to create schema
                else:
                    try:
                        cursor.execute("CREATE SCHEMA {schema};".format(schema = schema))
                    except psycopg2.Error as err:
                        print("Unable to create the new schema")
                        print(err)
                        print("Please try again")
                        # break will exit inner while loop and will enter the outer while loop
                        break
                    # end except
                    
                    # Successful creation
                    else:
                        print("Schema {schema} created sucessfully.".format(schema = schema))
                        # set schema_exists to True, then break inner while loop, outer while loop will stop because while not schema_exists will return bool False
                        schema_exists = True
                        break
                    # end try/fail/else
                # end else
            # end schema creation else
        # end inner while loop
    # end if sch_input
        
    # users schema name fails criteria.
    else:
        print("So sorry, this script only supports schemas starting with a letter and consisting of letters and numbers, please try again")
        # continues starts a new iteration of the while loop
        continue
    # end else
# end while not schema_exists

# initialize string of the PSQL statement that will create the table 
create_string = 'create table {schema}.'.format(schema = schema)

# prompts user to provide a table-name
while True:
    table_name = input("Please provide a name for your new table: ").strip()
    
    # table_name first character must be a letter and must consist only of letters and numbers.
    if table_name[0].isalpha() and table_name.isalnum():
        # add table_name to create_string
        create_string += table_name
        
        # break while loop
        break
    
    else:
        # table_name does not satisfy criteria, provide user with explanation
        print("Hey, thanks so much for using this script. We are looking for table names that start with a letter, with only letters and numbers.")
        
        # while loop starts over
        continue
# end of while loop

# add first parentheses to create_string before specifying columns
create_string += "("

# list containing the first line of text, where the column headers should be stored.
columns = records[0]

# create variable to store the index of the last index in columns[], for use later
last_col_index = len(columns) -1

# list to store what datatype a given column is.
data_types = []

# for loop goes through the content of columns[], adding information to the create_string 
for j in range(len(columns)):
    # assign current list value to col for easy reference, remove newlines if present.
    col = columns[j].strip("\n")
    
    # add column name + a whitespace to the create_string
    create_string += col + " "
    
    # prompt user for a datatype for column
    while True:
        # user has choice of 3 datatypes, input with characters 0-2
        col_datatype = input("Please supply a datatype for \"{col}\"(int = 0, varchar(25) = 1, varchar(50) = 2): ".format(col = col)).strip()
        
        # if/elif/elif/else perform actions based on whether 0, 1, 2 or something else is entered at the prompt
        if col_datatype  == "0":
            # make a note that int was selected in the data_types list
            data_types.append("int")
            
            # add text to create_string
            create_string += "int"
            
            # ask user if this is the primary key column
            while True:
                primary_key = input("Is this the primary key? (y/N): ").strip()
                
                # what to do if the int value is the primary key
                if primary_key.lower() == "y":
                    # add primary key text to create_string
                    create_string += " primary key not null"
                #end of if
                
                # break primary key while loop
                break
        
        elif col_datatype == "1":
            # make a note that a varchar value was selected in data_types list
            data_types.append("varchar")
            
            # add relevant varchar text to create_string
            create_string += "varchar(25)"
            
        elif col_datatype == "2":
            # make a note that a varchar value was selected in data_types list
            data_types.append("varchar")
            
            # add relevant varchar text to create_string
            create_string += "varchar(50)"
        
        else:
            # provide user with a friendly reminder to select a datatype within bounds
            print("Thank you so much for using this script. You didn't choose one of our supported datatypes, please try again")
            
            # restart while loop
            continue
        # end of if/elif/elif/else structure
        
        # logic for all columns except the last one
        if j != last_col_index:
            # insert comma to separate column name definitions
            create_string += ", "
        #end of if
        
        # break while loop for current column definition
        break
# column header definition for loop ends

# add last characters to complete table create_string
create_string += ");"

# try/except/else - create table and provide confirmation or provide human readable error message
try:
    cursor.execute(create_string)
    print(create_string)
    
except psycopg2.Error as err:
    print("Oh, we're sorry, looks like we weren't able to create your table :/")
    # print create_string so it can be troubleshooted
    print(create_string)
    # no point in continuing if table isn't created, exiting script
    exit()
    
else:
    print("Table succesfully created!")
    # Had to commit these changes to get rid of 'relation "schema_name.table_name" does not exist' error while inserting further down.
    connection.commit()
    connection.close()
# end of try/except/else

# begin insert_string creation
insert_string = 'insert into {schema}.{table_name} values '.format(schema = schema, table_name = table_name)

# create variable to store the index of the last index in records[], for use later
last_record_index = (len(records) -1)

# adds records from records[], starting with index 1, to insert_string
for k in range(1, len(records)):
    # store current record for easy reference.
    record = records[k]
    
    # add parentheses to enclose values
    insert_string += "("
    
    # create variable to store index of the last value in record[]
    last_val_index = len(record) -1

    # adds values to insert_string
    for l in range(len(record)):
        # store current value in val for easy reference
        val = record[l]
        
        # if/else check if value is varchar
        if data_types[l] == 'varchar':
            # add quotes to value before adding it to insert_string
            insert_string += "'{val}'".format(val = val)
        
        else:
            # add value without quotes
            insert_string += "{val}".format(val = val)
        # end of if/else
        
        
        # actions to perform for all values except the last one
        if l != last_val_index:
            #separate from next value with a comma
            insert_string += ","
        # end of if
    # end of record for-loop
    
    # add second parentheses to enclose value set
    insert_string += ")"
        
    # perform action for all records except last one
    if k != last_record_index:
        # separate from next record with comma
        insert_string += ","
    # end of if
# end of reords for-loop

# add last character to finish insert_string PSQL statement
insert_string += ";"

connection = db_connect(database, user, password, host, port)

cursor = connection.cursor()

# try/except/fail - inserts values and provides confirmation, or provides human readable error if insertion is unsuccessful
try:
    cursor.execute(insert_string)
except psycopg2.Error as err:
    print("An error was generated while inserting the records!")
    print(err)
    
else:
    print("Records inserted successfully!\n")
# end try/except fail

# create PSQL statement to retrieve lines written to database table
select_string = "SELECT * FROM {schema}.{table_name};".format(schema = schema, table_name = table_name)

# execute statement
cursor.execute(select_string)

# print each line in list format
for written_record in cursor.fetchall():
    print(written_record)
# end of for

print("The above records have been written to the your table.")

# ask user if the changes should be saved or discarded
commit = input("Would you like to commit all these changes to the database? (y/N): ").strip()

# if/else if the answer is lower case y, save changes, if anything else discard changes
if commit.lower() == "y":
    connection.commit()
    print("Your changes have been commited to the database.")
else:
    # rolling back inserts
    connection.rollback()
    try:
        # table was commited earlier, discarding it on user request
        cursor.execute("DROP TABLE {schema}.{table_name};".format(schema = schema, table_name = table_name))
    except psycopg2.Error as err:
        print("We weren't able to drop the table, sorry")
    else:
        # commit, ironically, to ensure table is dropped. 
        connection.commit()
        print("Totally understand, all changes have been rolled back, no changes made to the database")

# close connection to database. 
connection.close()

# bid the user farewell
print("Thank you for using this script!")
