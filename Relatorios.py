import pandas as pd
import ConexaoPostgreRailway
import numpy
import time


# CLASSE COM AS FUNÇOES PARA INTERAGIR COM AS APIS DE ACESSO DA "REPOSICAO"
def relatorioEndereços ():
    conn = ConexaoPostgreRailway.conexao()
    relatorioEndereço = pd.read_sql('select "Endereco","CodReduzido" ,"Engenharia" , count(codbarrastag) as saldo, "Descricao", cor , tamanho     from "Reposicao".tagsreposicao t   '
                                    'group by "Endereco", "CodReduzido" , "Engenharia" ,"Descricao", cor , tamanho   ',conn)
    conn.close()
    return relatorioEndereço


