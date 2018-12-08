# pypsqlimport.py creates table based on a given delimited text file to a database

# Imports PostgreSQL library
import psycopg2

# try/except/else attempt connection to database, generate a human readable error message if attempt is unsuccessful.
try:
    connection = psycopg2.connect(database = "staff", user = "python", password = "Password.", host = "127.0.0.1", port = "5432")

except psycopg2.Error as err:
    print("An error was generated!")
    print(err)
    
else:
    print("Connection to database was successful!")

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
# end of while loop

# list - store records for import into mystaff.employees
records = []

# go through textfile line by line
for i in f.readlines() :
    # append each textfile line to records list in as a list using "/ " to delimit values
    records.append(i.split("/ "))

# end of f.readlines() for-loop

# close textfile
f.close()

# initialize string of the PSQL statement that will create the table 
create_string = "create table mystaff."

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

# cursor needed to execute PSQL commands.
cursor = connection.cursor()

# try/except/else - create table and provide confirmation or provide human readable error message
try:
    cursor.execute(create_string)
    
except psycopg2.Error as err:
    print("Oh, we're sorry, looks like we weren't able to create your table :/")
    # print create_string so it can be troubleshooted
    print(create_string)
    # no point in continuing if table isn't created, exiting script
    exit()
    
else:
    print("Table succesfully created!")
# end of try/except/else

# begin insert_string creation
insert_string = "insert into mystaff.{table_name} values ".format(table_name = table_name)

# create variable to store the index of the last index in records[], for use later
last_record_index = (len(records) -1)

# preparing cursor needed to execute PSQL statements
cursor = connection.cursor()

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
select_string = "SELECT * FROM mystaff.{table_name};".format(table_name = table_name)

# cursor needed to execute PSQL statement
cursor = connection.cursor()

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
    connection.rollback()
    print("Totally understand, all changes have been rolled back, no changes made to the database")

# close connection to database. 
connection.close()

# bid the user farewell
print("Thank you for using this script!")
