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
def PesquisaEnderecosSKU(codreduzido):
    conn = ConexaoPostgreRailway.conexao()
    sku = pd.read_sql(
        ' select distinct  "Endereco" from "Reposicao"."tagsreposicao" '
        ' where "CodReduzido"= '+"'"+ codreduzido+"'", conn)

    if sku.empty :
        return False
    else:

        return sku

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
    
def Devolver_Inf_Tag(codbarras, padrao = 0):
    conn = ConexaoPostgreRailway.conexao()
    codReduzido = pd.read_sql(
        'select "codReduzido", "CodEngenharia","Situacao", "Usuario","numeroop", "descricao", "Cor","epc", "tamanho", "totalop"  from "Reposicao"."filareposicaoportag" ce '
        'where "codbarrastag" = '+"'"+codbarras+"'", conn)
    TagApontadas = pd.read_sql('select count("codbarrastag") as situacao, "CodReduzido", "Engenharia", "numeroop", "Descricao", "cor", "Epc", "tamanho", "totalop" from "Reposicao"."tagsreposicao" tr '
                               'where"codbarrastag" = '+"'"+codbarras+"'"+
                               ' group by "codbarrastag","CodReduzido", "Engenharia", "numeroop", "Descricao", "cor", "Epc", "tamanho", "totalop"   ',conn)

    conn.close()
    if not TagApontadas.empty and TagApontadas["situacao"][0] >= 0 and padrao == 0:
        return 'Reposto',TagApontadas['CodReduzido'][0] , TagApontadas['Engenharia'][0],TagApontadas['numeroop'][0],TagApontadas['Descricao'][0],TagApontadas['cor'][0], \
                TagApontadas['Epc'][0],TagApontadas['tamanho'][0],TagApontadas['totalop'][0]
    if padrao == 1:
        conn = ConexaoPostgreRailway.conexao()
        Usuario = pd.read_sql('select "Usuario" from "Reposicao"."filareposicaoportag" ce' \
                  ' where "numeroop" = '+"'"+TagApontadas['numeroop'][0]+"'", conn)
        conn.close()
        if not Usuario.empty:
            return 'Reposto', TagApontadas['CodReduzido'][0], TagApontadas['Engenharia'][0], TagApontadas['numeroop'][0], \
            TagApontadas['Descricao'][0], TagApontadas['cor'][0], \
                TagApontadas['Epc'][0], TagApontadas['tamanho'][0], TagApontadas['totalop'][0], Usuario['Usuario'][0]
        else:
            return 'Reposto', TagApontadas['CodReduzido'][0], TagApontadas['Engenharia'][0], TagApontadas['numeroop'][0], \
            TagApontadas['Descricao'][0], TagApontadas['cor'][0], \
                TagApontadas['Epc'][0], TagApontadas['tamanho'][0], TagApontadas['totalop'][0], "-"

    if codReduzido.empty:
        return False, pd.DataFrame({'Status': [True], 'Mensagem': [f'codbarras {codbarras} encontrado!']}), False, False,False,False,False,False, False

    else:
        return codReduzido['codReduzido'][0], codReduzido['CodEngenharia'][0], codReduzido['Usuario'][0], \
        codReduzido['numeroop'][0], codReduzido['descricao'][0], codReduzido['Cor'][0], codReduzido['epc'][0],codReduzido['tamanho'][0],codReduzido['totalop'][0]



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
    reduzido, codEngenharia, usuario, numeroop, descricao, cor, epc, tam, totalop = Devolver_Inf_Tag(codbarras)
    if reduzido == False:
         return False
    if reduzido == 'Reposto':
        return 'Reposto'
    else:
        #insere os dados da reposicao
        Insert = ' INSERT INTO "Reposicao"."tagsreposicao" ("Usuario","codbarrastag","Endereco","DataReposicao","CodReduzido","Engenharia","numeroop","Descricao", ' \
                 '"cor", "Epc", "tamanho","totalop" )' \
                 ' VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'
        cursor = conn.cursor()
        cursor.execute(Insert
                       , (usuario, codbarras, endereco,dataHora,reduzido,codEngenharia,numeroop,descricao,cor,epc,tam,totalop))

        # Obter o número de linhas afetadas
        numero_linhas_afetadas = cursor.rowcount
        conn.commit()
        cursor.close()
        print('teste')
        return  numero_linhas_afetadas
def EstornoApontamento(codbarrastag):
    conn = ConexaoPostgreRailway.conexao()
    situacao, reduzido, codEngenharia, numeroop, descricao, cor, epc, tam, totalop, usuario = Devolver_Inf_Tag(codbarrastag, 1)
    Insert = 'INSERT INTO  "Reposicao"."filareposicaoportag" ("codReduzido", "CodEngenharia","codbarrastag","numeroop", "descricao", "Cor", "epc", "tamanho", "totalop", "Situacao", "Usuario") ' \
             'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'+"'Reposição não Iniciada'"+',%s);'
    cursor = conn.cursor()
    cursor.execute(Insert
                   , (reduzido, codEngenharia, codbarrastag, numeroop, descricao, cor, epc, tam, totalop, usuario))
    # Obter o número de linhas afetadas
    numero_linhas_afetadas = cursor.rowcount
    conn.commit()
    cursor.close()
    # Apagando a Tag estorna
    cursor = conn.cursor()
    delete = 'Delete from "Reposicao"."tagsreposicao"  ' \
             'where "codbarrastag" = %s;'
    cursor.execute(delete
                   , (codbarrastag,))
    conn.commit()
    cursor.close()
    conn.close()

    return True
