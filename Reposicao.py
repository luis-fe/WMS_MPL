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
    if reduzido == False:
         return False
    if reduzido == 'Reposto':
        return 'Reposto'
    else:
        #insere os dados da reposicao
        Insert = ' INSERT INTO "Reposicao"."TagsReposicao" ("Usuario","codbarrastag","Endereco","DataReposicao","CodReduzido","Engenharia")' \
                 ' VALUES (%s,%s,%s,%s,%s,%s);'
        cursor = conn.cursor()
        cursor.execute(Insert
                       , (codUsuario, codbarras, endereco,dataHora,reduzido,codEngenharia))

        # Obter o número de linhas afetadas
        numero_linhas_afetadas = cursor.rowcount
        conn.commit()
        cursor.close()
        cursor = conn.cursor()

        # AQUI FAZ O UPDATE DA TAG NA FILA DAS TAGS:
        Situacao = 'Reposto'
        uptade = 'UPDATE "Reposicao"."FilaReposicaoporTag" ' \
                 'SET "Situacao"= %s ' \
                 'WHERE "codbarrastag"= %s;'
        cursor.execute(uptade
                       , (Situacao, codbarras))
        conn.commit()
        cursor.close()

        #AQUI IREMOS ATUALIZART O ESTOQUE DO ENDEREÇO E CODIGO DE BARRAS
            #1.1 procuro se ja existe o ID codReduzido||endereco

        saldo = Pesquisa_Estoque(reduzido, endereco)
        if saldo == False:
            ID = reduzido + '||' + endereco
            estoqueInsert = 'INSERT INTO "Reposicao"."Estoque" ("codreduzido", "endereco", "Saldo", "id")' \
                            'VALUES (%s, %s, %s, %s);'
            cursor = conn.cursor()
            cursor.execute(estoqueInsert, (reduzido, endereco, 1, ID))
            conn.commit()
            cursor.close()
            return True
        else:
            ID = reduzido + '||' + endereco
            saldo1 = saldo + 1
            estoqueUPDATE = 'UPDATE "Reposicao"."Estoque" ' \
                            'SET "Saldo" = %s ' \
                            'WHERE "id" = %s;'
            cursor = conn.cursor()
            cursor.execute(estoqueUPDATE, (int(saldo1), ID))
            conn.commit()
            cursor.close()
        conn.close()
        return  numero_linhas_afetadas

def Devolver_Inf_Tag(codbarras):
    conn = ConecaoAWSRS.conexao()
    codReduzido = pd.read_sql(
        'select "codReduzido", "CodEngenharia", "Situacao"  from "Reposicao"."FilaReposicaoporTag" ce '
        'where "codbarrastag" = '+"'"+codbarras+"'", conn)
    TagApontadas = pd.read_sql('select count("codbarrastag") as situacao from "Reposicao"."TagsReposicao" tr '
                               'where"codbarrastag" = '+"'"+codbarras+"'"+
                               ' group by "codbarrastag" ',conn)

    conn.close()
    if codReduzido.empty:
        return False, pd.DataFrame({'Status': [True], 'Mensagem': [f'codbarras {codbarras} encontrado!']})
    if codReduzido["Situacao"][0]=='Reposto':
        return 'Reposto', pd.DataFrame({'Status': [True], 'Mensagem': [f'codbarras {codbarras} encontrado!']})
    else:
        return codReduzido['codReduzido'][0], codReduzido['CodEngenharia'][0]

def Pesquisa_Estoque(reduzido, endereco):
    conn = ConecaoAWSRS.conexao()
    estoque = pd.read_sql(
        'select "Saldo"  from "Reposicao"."Estoque" e '
        'where "codreduzido" = '+"'"+reduzido+"'"+' and "endereco"= ' +"'"+endereco+"'", conn)
    conn.close()
    if estoque.empty:
        return False
    else:
        return estoque['Saldo'][0]
def SituacaoEndereco(endereco):
    conn = ConecaoAWSRS.conexao()
    select = 'select * from "Reposicao"."cadEndereco" ce ' \
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
            skus = pd.read_sql('select  "CodReduzido", "Saldo"  from "Reposicao"."Estoque" e '
                                    'where "Endereco"='+" '"+endereco+"'",conn)
            conn.close()
            skus['enderco'] = endereco
            skus['Status Endereco'] = True
            skus['Mensagem'] = f'Endereço {endereco} existe!'
            skus['Status do Saldo']='Cheio'
            return skus

def Estoque_endereco(endereco):
    conn = ConecaoAWSRS.conexao()
    consultaSql = 'select "Saldo" from "Reposicao"."Estoque" e ' \
                  'where "endereco" = %s'
    cursor = conn.cursor()
    cursor.execute(consultaSql, (endereco,))
    resultado = cursor.fetchall()
    cursor.close()
    if not resultado:
        return 0
    else:
        return resultado[0][0]
