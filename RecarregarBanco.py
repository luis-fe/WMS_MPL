import pandas as pd
import psycopg2
import pyodbc
import time
from sqlalchemy import create_engine
import datetime


def obterHoraAtual():
    agora = datetime.datetime.now()
    hora_str = agora.strftime('%d/%m/%Y %H:%M')
    return hora_str

def Funcao_Inserir (df_tags, tamanho):
    # Configurações de conexão ao banco de dados
    database = "wms_bd"
    user = "wms"
    password = "Master100"
    host = "wmsbd.cyiuowfro4wv.sa-east-1.rds.amazonaws.com"
    port = "5432"

# Cria conexão ao banco de dados usando SQLAlchemy
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

    # Inserir dados em lotes
    chunksize = tamanho
    for i in range(0, len(df_tags), chunksize):
        df_tags.iloc[i:i + chunksize].to_sql('FilaReposicaoTags', engine, if_exists='replace', index=False , schema='Reposicao')

start_time = time.perf_counter()


def FilaTags():
    conn = pyodbc.connect(dsn='SISTEMAS CSW', user='root', password='ccscache')
    conn2 = psycopg2.connect(host="wmsbd.cyiuowfro4wv.sa-east-1.rds.amazonaws.com", database="wms_bd", user="wms",
                             password="Master100")
    df_tags = pd.read_sql(
        "SELECT codBarrasTag as codbarrastag, codNaturezaAtual , codEngenharia , codReduzido,(SELECT i.nome  FROM cgi.Item i WHERE i.codigo = t.codReduzido) as descricao ,numeroOP"
        " from tcr.TagBarrasProduto t WHERE codEmpresa = 1 and codNaturezaAtual = 5 and situacao = 3", conn)

    df_tags['codNaturezaAtual'] = df_tags['codNaturezaAtual'].astype(str)




    # CRIANDO O DATAFRAME DO QUE JA FOI REPOSTO E USANDO MERGE
    ESTOQUE = pd.read_sql('select "Usuario", "Codigo_Barras" as codbarrastag  from "Reposicao"."TagsReposicao" ',conn2)
    df_tags = pd.merge(df_tags,ESTOQUE,on='codbarrastag',how='left')
    df_tags['Situacao'] = df_tags.apply(lambda row: 'Reposto' if not pd.isnull(row['Usuario']) else 'Reposição não Iniciada', axis=1)
    epc = LerEPC()
    df_tags = pd.merge(df_tags, epc, on='codbarrastag', how='left')
    df_tags.rename(columns={'codbarrastag': 'codBarrasTag','codEngenharia':'CodEngenharia'
                            , 'numeroOP':'numeroOp'}, inplace=True)
    conn2.close()
    df_tags['epc'] = df_tags['epc'].str.extract('\|\|(.*)').squeeze()
    print(df_tags.dtypes)
    print(df_tags['codBarrasTag'].size)
    tamanho = df_tags['codBarrasTag'].size
    dataHora = obterHoraAtual()
    df_tags['DataHora'] = dataHora
    Funcao_Inserir(df_tags, tamanho)


    conn.close()

def LerEPC():
    conn = pyodbc.connect(dsn='SISTEMAS CSW', user='root', password='ccscache')


    consulta = pd.read_sql('select epc.id as epc, t.codBarrasTag as codbarrastag from tcr.SeqLeituraFase  t '
                           'join Tcr_Rfid.NumeroSerieTagEPC epc on epc.codTag = t.codBarrasTag '
                           'WHERE t.codEmpresa = 1 and (t.codTransacao = 3500 or t.codTransacao = 501) '
                           'and (codLote like "23%" or  codLote like "24%" or codLote like "25%" '
                           'or codLote like "22%" )',conn)
    conn.close()
    print(consulta)
    return consulta


'''''
print(f"O trecho de código levou {elapsed_time:.6f} segundos para ser executado")
while True:
    # insira aqui as instruções que deseja executar a cada intervalo de tempo
    print("Executando instruções...")

    # pausa o programa por 5 segundos
    time.sleep(5)
'''''



