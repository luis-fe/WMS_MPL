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
    
def Devolver_Inf_Tag(codbarras):
    conn = ConexaoPostgreRailway.conexao()
    codReduzido = pd.read_sql(
        'select "codReduzido", "CodEngenharia","Situacao", "Usuario","numeroop"  from "Reposicao"."filareposicaoportag" ce '
        'where "codbarrastag" = '+"'"+codbarras+"'", conn)
    TagApontadas = pd.read_sql('select count("codbarrastag") as situacao from "Reposicao"."tagsreposicao" tr '
                               'where"codbarrastag" = '+"'"+codbarras+"'"+
                               ' group by "codbarrastag" ',conn)

    conn.close()
    if TagApontadas["situacao"][0]>=0:
        return 'Reposto', pd.DataFrame({'Status': [True], 'Mensagem': [f'codbarras {codbarras} encontrado!']}),'Reposto','Reposto'

    if codReduzido.empty:
        return False, pd.DataFrame({'Status': [True], 'Mensagem': [f'codbarras {codbarras} encontrado!']}),False,False

    else:
        return codReduzido['codReduzido'][0], codReduzido['CodEngenharia'][0],codReduzido['Usuario'][0],codReduzido['numeroop'][0]

def Pesquisa_Estoque(reduzido, endereco):
    conn = ConexaoPostgreRailway.conexao()
    estoque = pd.read_sql(
        'select "Saldo"  from "Reposicao"."estoque" e '
        'where "codreduzido" = '+"'"+reduzido+"'"+' and "endereco"= ' +"'"+endereco+"'", conn)
    conn.close()
    if estoque.empty:
        return False
    else:
        return estoque['Saldo'][0]

def ApontarReposicao(codUsuario, codbarras, endereco, dataHora):
    conn = ConexaoPostgreRailway.conexao()
    #devolvendo o reduzido do codbarras
    reduzido, codEngenharia, usuario, numeroop = Devolver_Inf_Tag(codbarras)
    if reduzido == False:
         return False
    if reduzido == 'Reposto':
        return 'Reposto'
    else:
        #insere os dados da reposicao
        Insert = ' INSERT INTO "Reposicao"."tagsreposicao" ("Usuario","codbarrastag","Endereco","DataReposicao","CodReduzido","Engenharia","numeroop")' \
                 ' VALUES (%s,%s,%s,%s,%s,%s,%s);'
        cursor = conn.cursor()
        cursor.execute(Insert
                       , (usuario, codbarras, endereco,dataHora,reduzido,codEngenharia,numeroop))

        # Obter o número de linhas afetadas
        numero_linhas_afetadas = cursor.rowcount
        conn.commit()
        cursor.close()
        print('teste')
        return  numero_linhas_afetadas