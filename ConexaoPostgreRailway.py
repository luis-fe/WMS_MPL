import psycopg2

def conexao():
    db_name = "railway"
    db_user = "postgres"
    db_password = "TMecLDjZX4xMBqKd3hHY"
    db_host = "containers-us-west-152.railway.app"
    portbanco = "5848"
    
    return psycopg2.connect(database=db_name, user=db_user, password=db_password, host=db_host, port=portbanco)


