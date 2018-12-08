# pysqlimport_static.py - adds the values in a provided textfile to specified table

# Imports necessary modules for connecting to PostGreSQL databases 
import psycopg2

# try/except/else attempt connection to database, generate a human readable error message if attempt is unsuccessful.
try:
    connection = psycopg2.connect(database = "staff", user = "python", password = "Password.", host = "127.0.0.1", port = "5432")

except psycopg2.Error as err:
    print("An error was generated!")
    print(err)
    
else:
    print("Connection to database was successful!")

# open textfile with data to for mystaff.employees table
f = open("./employees.txt") 

# list - store records for import into mystaff.employees
records = [] 

# go through textfile line by line
for i in f.readlines() : 
    # append each textfile line to records list in as a list using "/ " to delimit values
    records.append(i.split("/ ")) 
# end of f.readlines() for-loop

# create string for PSQL insert statement
insert_string = "insert into mystaff.employees values" 

# create variable for use in writing the PSQL insert statement
last_record_index = (len(records) -1) 

# cursor is needed to execute PSQL commands
cursor = connection.cursor() 

# add values to insert_string by way of for-loop
for i in range(len(records)): 
    record = records[i]
    # each record has it's values assigned to the correct database column
    insert_string += "({idno}, '{first_name}', '{last_name}','{department}', '{phone_number}', '{address}', {salary})".format(idno = record[0], first_name = record[1], last_name = record[2], department = record[3], phone_number = record[4], address = record[5], salary = record[6].strip("\n")) 
    
    # execute code for record that isn't the last record in records[]
    if i != last_record_index: 
        # add comma before the next set of values from records[]
        insert_string += "," 
# add values for loop ends

# add last character to insert_string for valid PSQL statement
insert_string += ";" 

#try/except/else - add values to database table and print confirmation message or produce machine readable error message if unsuccessful
try: 
    cursor.execute(insert_string)
except psycopg2.Error as err:
    print("An error was generated while inserting the records!")
    print(err)
    
else:
    print("Records inserted successfully!\n")
    
# commit all changes to database
connection.commit()
# close connection to database 
connection.close() 
