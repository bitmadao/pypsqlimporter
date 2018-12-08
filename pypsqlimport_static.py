import psycopg2

try:
    connection = psycopg2.connect(database = "staff", user = "python", password = "Password.", host = "127.0.0.1", port = "5432")

except psycopg2.Error as err:
    print("An error was generated!")
    print(err)
    
else:
    print("Connection to database was successful!")

f = open("./employees.txt")

records = []

for i in f.readlines() :
    records.append(i.split("/ "))
    
insert_string = "insert into mystaff.employees values"
last_record_index = (len(records) -1)

cursor = connection.cursor()

for i in range(len(records)):
    record = records[i]
    insert_string += "({idno}, '{first_name}', '{last_name}','{department}', '{phone_number}', '{address}', {salary})".format(idno = record[0], first_name = record[1], last_name = record[2], department = record[3], phone_number = record[4], address = record[5], salary = record[6].strip("\n"))
    if i != last_record_index:
        insert_string += ","

insert_string += ";"
try:
    cursor.execute(insert_string)
except psycopg2.Error as err:
    print("An error was generated while inserting the records!")
    print(err)
    
else:
    print("Records inserted successfully!\n")
    

connection.commit()
connection.close()
