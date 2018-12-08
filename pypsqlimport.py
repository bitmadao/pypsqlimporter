import psycopg2

try:
    connection = psycopg2.connect(database = "staff", user = "python", password = "Password.", host = "127.0.0.1", port = "5432")

except psycopg2.Error as err:
    print("An error was generated!")
    print(err)
    
else:
    print("Connection to database was successful!")

while True:
    filepath = input("Please provide a filename: ").strip()
    
    try:
        f = open(filepath)
    except FileNotFoundError as err:
        print("Terribly sorry, we can't make sense of this filename/path. Please try again.")
        continue
    else:
        print("File loaded successfully")
        break

records = []

for i in f.readlines() :
    records.append(i.split("/ "))

f.close()
create_string = "create table mystaff."

while True:
    table_name = input("Please provide a name for your new table: ").strip()
    
    if table_name[0].isalpha() and table_name.isalnum():
        create_string += table_name
        break
    
    else:
        print("Hey, thanks so much for using this script. We are looking for table names that start with a letter, with only letters and numbers.")
        continue

create_string += "("

columns = records[0]
last_col_index = len(columns) -1
data_types = []
for j in range(len(columns)):
    col = columns[j].strip("\n")
    create_string += col + " "
    while True:
        col_datatype = input("Please supply a datatype for \"{col}\"(int = 0, varchar(25) = 1, varchar(50) = 2): ".format(col = col)).strip()
        
        if col_datatype  == "0":
            data_types.append("int")
            create_string += "int"
            
            while True:
                primary_key = input("Is this the primary key? (y/N): ").strip()
                
                if primary_key.lower() == "y":
                    create_string += " primary key not null"
                
                break
            
        elif col_datatype == "1":
            data_types.append("varchar")
            create_string += "varchar(25)"
            
        elif col_datatype == "2":
            data_types.append("varchar")
            create_string += "varchar(50)"
        
        else:
            print("Thank you so much for using this script. Unfortunately we don't support this datatype, please try again")
            continue
    
        if j != last_col_index:
            create_string += ", "
        break

create_string += ");"

cursor = connection.cursor()

try:
    cursor.execute(create_string)
    
except psycopg2.Error as err:
    print("Oh, we're sorry, looks like we weren't able to create your table :/")
    print(create_string)
    exit()
    
else:
    print("Table succesfully created!")


insert_string = "insert into mystaff.{table_name} values ".format(table_name = table_name)

last_record_index = (len(records) -1)

cursor = connection.cursor()

for k in range(1, len(records)):
    record = records[k]
    
    insert_string += "("
    
    last_val_index = len(record) -1

    for l in range(len(record)):
        val = record[l]
        
        if data_types[l] == 'varchar':
            insert_string += "'{val}'".format(val = val)
        
        else:
            insert_string += "{val}".format(val = val)
        
        if l != last_val_index:
            insert_string += ","
    
    insert_string += ")"
        
    if k != last_record_index:
        insert_string += ","

insert_string += ";"
try:
    cursor.execute(insert_string)
except psycopg2.Error as err:
    print("An error was generated while inserting the records!")
    print(err)
    
else:
    print("Records inserted successfully!\n")
    
select_string = "SELECT * FROM mystaff.{table_name};".format(table_name = table_name)

cursor = connection.cursor()

cursor.execute(select_string)
for written_record in cursor.fetchall():
    print(written_record)

print("The above records have been written to the your table")

commit = input("Would you like to commit all these changes to the database? (y/N): ").strip()

if commit.lower() == "y":
    connection.commit()
    print("Your changes have been commited to the database.")
else:
    connection.rollback()
    print("Totally understand, all changes have been rolled back, no changes made to the database")

connection.close()
print("Thank you for using this script!")
