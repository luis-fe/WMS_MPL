import psycopg2

def conexao():
    db_name = "wms_bd"
    db_user = "wms"
    db_password = "Master100"
    db_host = "wmsbd.cyiuowfro4wv.sa-east-1.rds.amazonaws.com"
    portbanco = "5432"
    
    return psycopg2.connect(database=db_name, user=db_user, password=db_password, host=db_host, port=portbanco)
