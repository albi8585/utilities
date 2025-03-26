#questo modulo lavora con connessione sqlAlchemy perchè la funzione pandas "to_sql" 
# per poter caricare il dataframe su database funziona solo con l'oggetto connessione di questa libreria.
#è comodo perchè basta creare il nome della tabella e la funzione "to_sql" automaticamente crea la tabella o fa il replace 
# se già esistente, crea le variabili della tabella con i loro attributi e inserisce i valori dei dataframe. Ad ora non conosco i contro


from carica_query_delivered import carica_query_delivered
import mysql.connector as msc   
import pandas as pd
from connessione_alchemy import connessionealchemy

lista_query = carica_query_delivered()
db_alchemy = connessionealchemy()




for i in range(len(lista_query)):
    df = pd.read_sql(lista_query[i],db_alchemy) 
    print(df)
    ind = str(i)
    tab = "ecommerce.userid" + ind      
    df.to_sql(f"{tab}",db_alchemy,if_exists='replace',index=False) #FUNZIONA SOLO CON SQLaLCHEMY ,if_exists='replace'
    