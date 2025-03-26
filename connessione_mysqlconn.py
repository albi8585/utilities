
import mysql.connector
from carica_query_delivered import dbms

def conn_mysqlconn():
    database = dbms()
    if database == "mysql":
        try:
            mydb = mysql.connector.connect(
                host = 'localhost',  # Replace with your MySQL host
                user="root",  # Replace with your MySQL username
                password = 'dandito85',  # Replace with your MySQL password
                database = 'ecommerce' # Replace with your database name (optional)
            )

            print("Connection successful!")

            # Create a cursor object to execute SQL queries
            
            return mydb      

        except mysql.connector.Error as err:
            print(f"Error: {err}")
        
    return None

