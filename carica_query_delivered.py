

def carica_query_delivered ():

    
    a = "SELECT user_id, order_id,  created_at FROM ecommerce.order_items where year(created_at) = 2025" 
    b = "SELECT user_id, order_id,  created_at FROM ecommerce.order_items where year(created_at) = 2024" 
    c = "SELECT user_id, order_id,  created_at FROM ecommerce.order_items where year(created_at) = 2024" 
    return [a,b,c] 

def dbms ():
    dbms = "mysql"
    return dbms



  


