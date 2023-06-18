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
    relatorioFila = pd.read_sql('select * from "Reposicao".filareposicaoportag t ',conn)
    conn.close()
    return relatorioFila

def relatorioTotalFila ():
    conn = ConexaoPostgreRailway.conexao()
    relatorioFila = pd.read_sql('select count(codbarrastag) as SaldoFila from "Reposicao".filareposicaoportag t ',conn)
    conn.close()
    return relatorioTotalFila

