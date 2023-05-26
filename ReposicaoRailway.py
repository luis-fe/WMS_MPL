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
    
def SituacaoEndereco(endereco):
    conn = ConexaoPostgreRailway.conexao()
    select = 'select * from "Reposicao"."cadendereco" ce ' \
             'where codendereco = %s'
    cursor = conn.cursor()
    cursor.execute(select, (endereco, ))
    resultado = cursor.fetchall()
    cursor.close()
    if not resultado:
        conn.close()
        return pd.DataFrame({'Status Endereco': [False], 'Mensagem': [f'endereco {endereco} nao existe!']})
    else:
        saldo = Estoque_endereco(endereco)
        if saldo == 0:
            conn.close()
            return pd.DataFrame({'Status Endereco': [True], 'Mensagem': [f'endereco {endereco} existe!'],
                                 'Status do Saldo': ['Vazio']})
        else:
            skus = pd.read_sql('select  "codreduzido", "Saldo"  from "Reposicao"."estoque" e '
                                    'where "endereco"='+" '"+endereco+"'",conn)
            conn.close()
            skus['enderco'] = endereco
            skus['Status Endereco'] = True
            skus['Mensagem'] = f'Endereço {endereco} existe!'
            skus['Status do Saldo']='Cheio'
            return skus

def Estoque_endereco(endereco):
    conn = ConexaoPostgreRailway.conexao()
    consultaSql = 'select "Saldo" from "Reposicao"."estoque" e ' \
                  'where "endereco" = %s'
    cursor = conn.cursor()
    cursor.execute(consultaSql, (endereco,))
    resultado = cursor.fetchall()
    cursor.close()
    if not resultado:
        return 0
    else:
        return resultado[0][0]
