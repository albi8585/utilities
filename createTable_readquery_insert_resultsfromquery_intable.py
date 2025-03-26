#questo modulo tramite la connessione mysql.connector crea le tabelle legge ed esegue le query  creando  
#tramite funzione di mysqlconnector "fetchall" una lista di tuple corrispondenti a ogni loro riga
#andando a inserirle a ogni ciclo nei valori della tabella tramite l'insert into. 


from carica_query_delivered import carica_query_delivered
import mysql.connector as msc   
import pandas as pd
from connessione_mysqlconn import conn_mysqlconn


lista_query = carica_query_delivered()
mydb = conn_mysqlconn()

mycursor = mydb.cursor()

for i in range(len(lista_query)):
    lett = str(i)
    query= lista_query[i]
    print(query)
    table= "userid"+ lett
    mycursor.execute(f"DROP TABLE {table}")
    mycursor.execute(f"CREATE TABLE {table} (ID INT AUTO_INCREMENT PRIMARY KEY,  OrderID INT,user_id INT)")
    mycursor.execute(query)
    result = mycursor.fetchall()
        
    for riga in result:        
        mycursor.execute(f"INSERT INTO {table} (ID,OrderID,user_id) VALUES (null,'{riga[0]}','{riga[1]}')")
    mydb.commit()
    


   
