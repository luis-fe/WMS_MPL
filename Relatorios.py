import pandas as pd
import ConexaoPostgreRailway
import numpy
import time


# CLASSE COM AS FUNÇOES PARA INTERAGIR COM AS APIS DE ACESSO DA "REPOSICAO"
def relatorioEndereços ():
    conn = ConexaoPostgreRailway.conexao()
    relatorioEndereço = pd.read_sql('select "Endereco","codreduzido" ,"engenharia" , count(codbarrastag) as saldo, "descricao", cor , tamanho     from "Reposicao".tagsreposicao t   '
                                    'group by "Endereco", "codreduzido" , "engenharia" ,"descricao", cor , tamanho   ',conn)
    conn.close()
    return relatorioEndereço

def relatorioFila ():
    conn = ConexaoPostgreRailway.conexao()
    relatorioFila = pd.read_sql('select "numeroop", count(codbarrastag) as Saldo from "Reposicao".filareposicaoportag t '
                                'group by "numeroop" ',conn)
    conn.close()
    return relatorioFila



def relatorioTotalFila():
    conn = ConexaoPostgreRailway.conexao()
    query = pd.read_sql('SELECT numeroop, COUNT(codbarrastag) AS Saldo '
        'FROM "Reposicao".filareposicaoportag t' 
        ' GROUP BY "numeroop" ',conn)

    query = query['saldo'].sum()
    conn.close()
    return query



