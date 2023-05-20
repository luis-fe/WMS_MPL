import pandas as pd
import ConecaoAWSRS
import numpy
import time
# CLASSE COM AS FUNÇOES PARA INTERAGIR COM AS APIS DE ACESSO DA "REPOSICAO"

def ObeterEnderecos():
    conn = ConecaoAWSRS.conexao()
    endercos = pd.read_sql(
        ' select * from "Reposicao"."cadEndereco" ce   ', conn)
    return endercos

def PesquisaEndereco(endereco):
    conn = ConecaoAWSRS.conexao()
    endercos = pd.read_sql(
        ' select * from "Reposicao"."cadEndereco" ce  where "codendereco"= '+"'"+endereco+"'", conn)
    if endercos.empty:
        return pd.DataFrame({'Status': [False], 'Mensagem': [f'endereco {endereco} não cadastrado']})
    else:

        return pd.DataFrame({'Status': [True], 'Mensagem': [f'endereco {endereco} encontrado!']})

def ApontarReposicao(codUsuario, codbarras, endereco, dataHora):
    conn = ConecaoAWSRS.conexao()
    #devolvendo o reduzido do codbarras
    reduzido, codEngenharia = Devolver_Inf_Tag(codbarras)
    #insere os dados da reposicao
    Insert = ' INSERT INTO "Reposicao"."TagsReposicao" ("Usuario","Codigo_Barras","Endereco","DataReposicao","CodReduzido","Engenharia")' \
             ' VALUES (%s,%s,%s,%s,%s,%s);'
    cursor = conn.cursor()
    cursor.execute(Insert
                   , (codUsuario, codbarras, endereco,dataHora,reduzido,codEngenharia))

    # Obter o número de linhas afetadas
    numero_linhas_afetadas = cursor.rowcount
    conn.commit()
    cursor.close()
    cursor = conn.cursor()

    # AQUI FAZ O UPDATE DA TAG NA FILA DAS TAGS "MANO":
    Situacao = 'Reposto'
    uptade = 'UPDATE "Reposicao"."FilaReposicaoporTag" ' \
             'SET "Situacao"= %s ,"Usuario"= %s ' \
             'WHERE "codBarrasTag"= %s;'
    cursor.execute(uptade
                   , (Situacao, codUsuario, codbarras))
    conn.commit()
    cursor.close()

    #AQUI IREMOS ATUALIZART O ESTOQUE DO ENDEREÇO E CODIGO DE BARRAS
        #1.1 procuro se ja existe o ID codReduzido||endereco

    saldo = Pesquisa_Estoque(reduzido, endereco)
    if saldo == False:
        ID = reduzido + '||' + endereco
        estoqueInsert = 'INSERT INTO "Reposicao"."Estoque" ("CodReduzido", "Endereco", "Saldo", "ID")' \
                        'VALUES (%s, %s, %s, %s);'
        cursor = conn.cursor()
        cursor.execute(estoqueInsert, (reduzido, endereco, 1, ID))
        conn.commit()
        cursor.close()
    else:
        ID = reduzido + '||' + endereco
        saldo1 = saldo + 1
        estoqueUPDATE = 'UPDATE "Reposicao"."Estoque" ' \
                        'SET "Saldo" = %s ' \
                        'WHERE "ID" = %s;'
        cursor = conn.cursor()
        cursor.execute(estoqueUPDATE, (int(saldo1), ID))
        conn.commit()
        cursor.close()
    conn.close()
    return  numero_linhas_afetadas

def Devolver_Inf_Tag(codbarras):
    conn = ConecaoAWSRS.conexao()
    codReduzido = pd.read_sql(
        'select "codReduzido", "CodEngenharia"  from "Reposicao"."FilaReposicaoporTag" ce '
        'where "codBarrasTag" = '+"'"+codbarras+"'", conn)

    conn.close()
    return codReduzido['codReduzido'][0], codReduzido['CodEngenharia'][0]

def Pesquisa_Estoque(reduzido, endereco):
    conn = ConecaoAWSRS.conexao()
    estoque = pd.read_sql(
        'select "Saldo"  from "Reposicao"."Estoque" e '
        'where "CodReduzido" = '+"'"+reduzido+"'"+' and "Endereco"= ' +"'"+endereco+"'", conn)
    conn.close()
    if estoque.empty:
        return False
    else:
        return estoque['Saldo'][0]
