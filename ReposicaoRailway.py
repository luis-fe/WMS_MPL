import pandas as pd
import ConexaoPostgreRailway
import numpy
import time

# CLASSE COM AS FUNÇOES PARA INTERAGIR COM AS APIS DE ACESSO DA "REPOSICAO"

def ObeterEnderecos():
    conn = ConexaoPostgreRailway.conexao()
    endercos = pd.read_sql(
        ' select * from "Reposicao"."cadendereco" ce   ', conn)
    return endercos

def PesquisaEndereco(endereco):
    conn = ConexaoPostgreRailway.conexao()
    endercos = pd.read_sql(
        ' select * from "Reposicao"."cadendereco" ce  where "codendereco"= '+"'"+endereco+"'", conn)
    if endercos.empty:
        return pd.DataFrame({'Status': [False], 'Mensagem': [f'endereco {endereco} não cadastrado']})

    else:

        return pd.DataFrame({'Status': [True], 'Mensagem': [f'endereco {endereco} encontrado!']})
