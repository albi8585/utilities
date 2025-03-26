from  carica_query_delivered import carica_query_delivered
import pandas as pd
from connessione_alchemy import connessionealchemy

def transform (query,conn):        
    df = pd.read_sql(query,conn)        
    df.loc[df.index  < 5, 'pippo'] = 'ciccio'        

    return df






    
    