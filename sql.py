import mysql.connector

mydb = mysql.connector.connect(
  host="your_hostname",
  port="your_port",
  user="your_username",
  password="your_password",
  database="your_database_name" # Optional: specify the database to connect to
)

print(mydb) # Check if the connection was successful
