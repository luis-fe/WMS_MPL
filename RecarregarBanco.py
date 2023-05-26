import pandas as pd
import pyodbc
import time
from sqlalchemy import create_engine
import datetime
import numpy
import ConexaoPostgreRailway

def obterHoraAtual():
    agora = datetime.datetime.now()
    hora_str = agora.strftime('%d/%m/%Y %H:%M')
    return hora_str

def Funcao_Inserir (df_tags, tamanho):
    # Configurações de conexão ao banco de dados
    database = "railway"
    user = "postgres"
    password = "TMecLDjZX4xMBqKd3hHY"
    host = "containers-us-west-152.railway.app"
    port = "5848"

# Cria conexão ao banco de dados usando SQLAlchemy
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

    # Inserir dados em lotes
    chunksize = tamanho
    for i in range(0, len(df_tags), chunksize):
        df_tags.iloc[i:i + chunksize].to_sql('filareposicaoportag', engine, if_exists='append', index=False , schema='Reposicao')

start_time = time.perf_counter()


def FilaTags():
    conn = pyodbc.connect(dsn='SISTEMAS CSW', user='root', password='ccscache')
    conn2 = ConexaoPostgreRailway.conexao()
    df_tags = pd.read_sql(
        "SELECT  codBarrasTag as codbarrastag, codNaturezaAtual , codEngenharia , codReduzido,(SELECT i.nome  FROM cgi.Item i WHERE i.codigo = t.codReduzido) as descricao , numeroop as numeroop,"
        " (SELECT i2.codCor  FROM cgi.Item2  i2 WHERE i2.Empresa = 1 and  i2.codItem  = t.codReduzido) as Cor,"
        " (SELECT tam.descricao  FROM cgi.Item2  i2 join tcp.Tamanhos tam on tam.codEmpresa = i2.Empresa and tam.sequencia = i2.codSeqTamanho  WHERE i2.Empresa = 1 and  i2.codItem  = t.codReduzido) as tamanho"
        " from tcr.TagBarrasProduto t WHERE codEmpresa = 1 and codNaturezaAtual = 5 and situacao = 3", conn)

    df_opstotal = pd.read_sql('SELECT top 200000 numeroOP as numeroop , totPecasOPBaixadas as totalop  '
                              'from tco.MovimentacaoOPFase WHERE codEmpresa = 1 and codFase = 236  '
                              'order by numeroOP desc ',conn)

    df_tags = pd.merge(df_tags, df_opstotal, on='numeroop', how='left')
    df_tags['totalop'] = df_tags['totalop'].replace('', numpy.nan).fillna('0')
    df_tags['codNaturezaAtual'] = df_tags['codNaturezaAtual'].astype(str)
    df_tags['totalop'] = df_tags['totalop'].astype(int)
    # CRIANDO O DATAFRAME DO QUE JA FOI REPOSTO E USANDO MERGE
       # Verificando as tag's que ja foram repostas
    TagsRepostas = pd.read_sql('select "codbarrastag" as codbarrastag, "Usuario" as usuario_  from "Reposicao"."tagsreposicao" tr ',conn2)
    df_tags = pd.merge(df_tags, TagsRepostas, on='codbarrastag', how='left')
    df_tags = df_tags.loc[df_tags['usuario_'].isnull()]
    df_tags.drop('usuario_', axis=1, inplace=True)
        # Verificando as tag's que ja estam na fila
    ESTOQUE = pd.read_sql('select "Usuario", "codbarrastag" as codbarrastag, "Situacao" as sti_aterior  from "Reposicao"."filareposicaoportag" ',conn2)
    df_tags = pd.merge(df_tags,ESTOQUE,on='codbarrastag',how='left')
    df_tags['Situacao'] = df_tags.apply(lambda row: 'Reposto' if not pd.isnull(row['Usuario']) else 'Reposição não Iniciada', axis=1)
    epc = LerEPC()
    df_tags = pd.merge(df_tags, epc, on='codbarrastag', how='left')
    df_tags.rename(columns={'codbarrastag': 'codbarrastag','codEngenharia':'CodEngenharia'
                            , 'numeroop':'numeroop'}, inplace=True)
    conn2.close()
    df_tags = df_tags.loc[df_tags['sti_aterior'].isnull()]
    # Excluir a coluna 'B' inplace
    df_tags.drop('sti_aterior', axis=1, inplace=True)
    df_tags['epc'] = df_tags['epc'].str.extract('\|\|(.*)').squeeze()
    print(df_tags.dtypes)
    print(df_tags['codbarrastag'].size)
    tamanho = df_tags['codbarrastag'].size
    dataHora = obterHoraAtual()
    df_tags['DataHora'] = dataHora
    df_tags.to_csv('planilha.csv')
    Funcao_Inserir(df_tags, tamanho)


    conn.close()
    conn2.close()
    return dataHora

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

