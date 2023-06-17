from datetime import datetime

import pandas as pd
import ConexaoPostgreRailway
import numpy
import time

def obterHoraAtual():
    agora = datetime.datetime.now()
    hora_str = agora.strftime('%Y-%m-%d %H:%M:%S')
    return hora_str


# CLASSE COM AS FUNÇOES PARA INTERAGIR COM AS APIS DE ACESSO DA "REPOSICAO"
def CadEndereco (rua, modulo, posicao):
    inserir = 'insert into "Reposicao".cadendereco ("codendereco","rua","modulo","posicao")' \
          ' VALUES (%s,%s,%s,%s);'
    codenderco = rua+"-"+modulo+"-"+posicao

    conn = ConexaoPostgreRailway.conexao()
    cursor = conn.cursor()
    cursor.execute(inserir
                   , (codenderco, rua, modulo, posicao))

    # Obter o número de linhas afetadas
    numero_linhas_afetadas = cursor.rowcount
    conn.commit()
    cursor.close()
    conn.close()
    return codenderco


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
            skus = pd.read_sql('select  count(codbarrastag) as "Saldo Geral"  from "Reposicao".tagsreposicao e '
                                    'where "Endereco"='+" '"+endereco+"'",conn)
            SaldoSku_Usuario = pd.read_sql('select  "Endereco", "CodReduzido" as codreduzido , "Usuario", count(codbarrastag) as "Saldo Sku"  from "Reposicao".tagsreposicao e '
                                    'where "Endereco"='+" '"+endereco+"'"
                                    'group by "Endereco", "CodReduzido" , "Usuario" ', conn)
            usuarios = pd.read_sql(
                'select codigo as "Usuario" , nome  from "Reposicao".cadusuarios c ',
                conn)
            usuarios['Usuario'] = usuarios['Usuario'].astype(str)
            SaldoSku_Usuario = pd.merge(SaldoSku_Usuario, usuarios, on='Usuario', how='left')
            SaldoSku_Usuario['Usuario'] = SaldoSku_Usuario["Usuario"] + '-'+SaldoSku_Usuario["nome"]
            SaldoSku_Usuario.drop('nome', axis=1, inplace=True)
            SaldoSku_Usuario.drop('Endereco', axis=1, inplace=True)

            skus['enderco'] = endereco
            skus['Status Endereco'] = True
            skus['Mensagem'] = f'Endereço {endereco} existe!'
            skus['Status do Saldo']='Cheio'
            SaldoGeral = skus['Saldo Geral'][0]

            detalhatag = pd.read_sql(
                'select codbarrastag, "Usuario", "CodReduzido" as codreduzido, "DataReposicao"  from "Reposicao".tagsreposicao t '
                'where "Endereco"='+" '"+endereco+"'"'',conn)
            detalhatag = pd.merge(detalhatag, usuarios, on='Usuario', how='left')
            conn.close()

            data = {
                '1- Endereço': f'{endereco} ',
                '2- Status Endereco':
                    True,
                '3- Mensagem': f'Endereço {endereco} existe!',
                '4- Status do Saldo': 'Cheio',
                '5- Saldo Geral': f'{SaldoGeral}',
                '6- Detalhamento Reduzidos': SaldoSku_Usuario.to_dict(orient='records'),
                '7- Detalhamento nivel tag':detalhatag.to_dict(orient='records')
            }

            return [data]



def Estoque_endereco(endereco):
    conn = ConexaoPostgreRailway.conexao()
    consultaSql = 'select count(codbarrastag) as "Saldo" from "Reposicao"."tagsreposicao" e ' \
                  'where "Endereco" = %s ' \
                  'group by "Endereco"'
    cursor = conn.cursor()
    cursor.execute(consultaSql, (endereco,))
    resultado = cursor.fetchall()
    cursor.close()
    if not resultado:
        return 0
    else:
        return resultado[0][0]

def Devolver_Inf_Tag(codbarras, padrao=0):
    conn = ConexaoPostgreRailway.conexao()
    cursor = conn.cursor()

    cursor.execute(
        'select "codReduzido", "CodEngenharia", "Situacao", "Usuario", "numeroop", "descricao", "Cor", "epc", "tamanho", "totalop"  from "Reposicao"."filareposicaoportag" ce '
        'where "codbarrastag" = %s', (codbarras,))
    codReduzido = pd.DataFrame(cursor.fetchall(), columns=['codReduzido', 'CodEngenharia', 'Situacao', 'Usuario', 'numeroop', 'descricao', 'Cor', 'epc', 'tamanho', 'totalop'])

    cursor.execute(
        'select count("codbarrastag") as situacao, "CodReduzido", "Engenharia", "numeroop", "Descricao", "cor", "Epc", "tamanho", "totalop" from "Reposicao"."tagsreposicao" tr '
        'where "codbarrastag" = %s '
        'group by "codbarrastag", "CodReduzido", "Engenharia", "numeroop", "Descricao", "cor", "Epc", "tamanho", "totalop"', (codbarras,))
    TagApontadas = pd.DataFrame(cursor.fetchall(), columns=['situacao', 'CodReduzido', 'Engenharia', 'numeroop', 'Descricao', 'cor', 'Epc', 'tamanho', 'totalop'])

    if not TagApontadas.empty and TagApontadas["situacao"][0] >= 0 and padrao == 0:
        retorno = (
            'Reposto',
            TagApontadas['CodReduzido'][0],
            TagApontadas['Engenharia'][0],
            TagApontadas['numeroop'][0],
            TagApontadas['Descricao'][0],
            TagApontadas['cor'][0],
            TagApontadas['Epc'][0],
            TagApontadas['tamanho'][0],
            TagApontadas['totalop'][0]
        )
    elif padrao == 1:
        cursor.execute('select "Usuario" from "Reposicao"."filareposicaoportag" ce where "numeroop" = %s', (TagApontadas['numeroop'][0],))
        Usuario = pd.DataFrame(cursor.fetchall(), columns=['Usuario'])

        if not Usuario.empty:
            retorno = (
                'Reposto',
                TagApontadas['CodReduzido'][0],
                TagApontadas['Engenharia'][0],
                TagApontadas['numeroop'][0],
                TagApontadas['Descricao'][0],
                TagApontadas['cor'][0],
                TagApontadas['Epc'][0],
                TagApontadas['tamanho'][0],
                TagApontadas['totalop'][0],
                Usuario['Usuario'][0]
            )
        else:
            retorno = (
                'Reposto',
                TagApontadas['CodReduzido'][0],
                TagApontadas['Engenharia'][0],
                TagApontadas['numeroop'][0],
                TagApontadas['Descricao'][0],
                TagApontadas['cor'][0],
                TagApontadas['Epc'][0],
                TagApontadas['tamanho'][0],
                TagApontadas['totalop'][0],
                "-"
            )
    elif codReduzido.empty:
        retorno = (
            False,
            pd.DataFrame({'Status': [True], 'Mensagem': [f'codbarras {codbarras} encontrado!']}),
            False, False, False, False, False, False, False
        )
    else:
        retorno = (
            codReduzido['codReduzido'][0],
            codReduzido['CodEngenharia'][0],
            codReduzido['Usuario'][0],
            codReduzido['numeroop'][0],
            codReduzido['descricao'][0],
            codReduzido['Cor'][0],
            codReduzido['epc'][0],
            codReduzido['tamanho'][0],
            codReduzido['totalop'][0]
        )

    cursor.close()
    conn.close()

    return retorno



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
        Insert = ' INSERT INTO "Reposicao"."tagsreposicao" ("Usuario","codbarrastag","Endereco","DataReposicao","CodReduzido","Engenharia","Descricao", ' \
                 '"cor", "Epc", "tamanho" )' \
                 ' VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'
        cursor = conn.cursor()
        cursor.execute(Insert
                       , (usuario, codbarras, endereco,dataHora,reduzido,codEngenharia,descricao,cor,epc,tam))

        # Obter o número de linhas afetadas
        conn.commit()
        cursor.close()

       # print(f'Apontado a {numeroop} , endereco {endereco}, as {dataHora}')

        return  True
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
