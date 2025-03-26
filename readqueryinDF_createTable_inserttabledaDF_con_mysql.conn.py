#questo modulo tramite mysql connector e pandas legge una query da database e la converte in dataframe. 
# Dopo eventuali modifiche in pandas crea una tabella e crea una lista di tuple da dataframe
# tramite la funzione pandas "itertuples" e le inserisce a ogni ciclo nei valori della tabella tramite insert into 


from carica_query_delivered import carica_query_delivered
import mysql.connector as msc   
import pandas as pd
from connessione_mysqlconn import conn_mysqlconn

lista_query = carica_query_delivered()
mydb = conn_mysqlconn()

mycursor = mydb.cursor()


for i in range(len(lista_query)):
    df = pd.read_sql(lista_query[i],mydb)     
    ind = str(i)
    tab = "ecommerce.userid" + ind
    #val = ('user_id INT', 'order_id INT') 
    mycursor.execute(f"DROP TABLE {tab}")
    qr = f"CREATE TABLE {tab} (user_id INT, order_id INT, created_at VARCHAR(50))"   
    mycursor.execute(qr)
    
    ##print(qr)
    #df.to_sql(f"{tab}",mydb,if_exists='replace',index=False) FUNZIONA SOLO CON SQLaLCHEMY
    #sql = f"INSERT INTO {tab} (user_id , order_id , created_at) VALUES (%i,%i,%s)"
    values = list(df.itertuples(index=False,name=None))
    print(values)  
    for riga in values:
        mycursor.execute(f"INSERT INTO {tab} (user_id,order_id,created_at) VALUES ({riga[0]},{riga[1]},'{riga[2]}')")
    mydb.commit()

    #mycursor.executemany(sql,values)
    #mydb.commit()
    