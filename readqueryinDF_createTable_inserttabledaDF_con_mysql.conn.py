#questo modulo tramite  pandas legge una query da database (con connessione sqlAlchemy) e la converte in dataframe con modifiche varie. 
# crea una tabella (con libreria mysql.connector) e crea una lista di tuple da dataframe
# tramite la funzione pandas "itertuples"  inserendole a ogni ciclo nei valori della tabella tramite insert into (con libreria mysql.connector) 

from transform_pandas import transform
from carica_query_delivered import carica_query_delivered
import mysql.connector as msc   
import pandas as pd
from connessione_mysqlconn import conn_mysqlconn
from connessione_alchemy import connessionealchemy

lista_query = carica_query_delivered()
mydb = conn_mysqlconn()
conn = connessionealchemy()

mycursor = mydb.cursor()


for i in range(len(lista_query)):
    df = transform(lista_query[i],conn)     
    ind = str(i)
    tab = "ecommerce.userid" + ind
    #val = ('user_id INT', 'order_id INT') 
    #mycursor.execute(f"DROP TABLE {tab}")
    qr = f"CREATE TABLE {tab} (user_id INT, order_id INT, created_at VARCHAR(50), pippo VARCHAR(50))"   
    mycursor.execute(qr)
    
    ##print(qr)
    #df.to_sql(f"{tab}",mydb,if_exists='replace',index=False) FUNZIONA SOLO CON SQLaLCHEMY
    #sql = f"INSERT INTO {tab} (user_id , order_id , created_at) VALUES (%i,%i,%s)"
    values = list(df.itertuples(index=False,name=None))
    print(values)  
    for riga in values:
        mycursor.execute(f"INSERT INTO {tab} (user_id,order_id,created_at,pippo) VALUES ({riga[0]},{riga[1]},'{riga[2]}','{riga[3]}')")
    mydb.commit()

    #mycursor.executemany(sql,values)
    #mydb.commit()
    
