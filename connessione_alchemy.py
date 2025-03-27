import pyodbc 
import mysql.connector
from carica_query_delivered import dbms
import pandas as pd
import sqlalchemy as sal
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError


def connessionealchemy ():
    database = dbms()
    if database == "mysql":
        try:
            username = "root" 
            password = 'xxxxxxx'
            host = 'localhost'
            database = 'ecommerce'
            port = "3306"
            stringaConnessione = f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}"
            conn = create_engine(stringaConnessione)
            return conn
        except SQLAlchemyError as e:
            print("errore:",e)
    return None

connessionealchemy()








   

