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
        ' where "codreduzido"= '+"'"+ codreduzido+"'", conn)

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
            SaldoSku_Usuario = pd.read_sql('select  "Endereco", "codreduzido" as codreduzido , "usuario", count(codbarrastag) as "Saldo Sku"  from "Reposicao".tagsreposicao e '
                                    'where "Endereco"='+" '"+endereco+"'"
                                    'group by "Endereco", "codreduzido" , "usuario" ', conn)
            usuarios = pd.read_sql(
                'select codigo as "usuario" , nome  from "Reposicao".cadusuarios c ',
                conn)
            usuarios['usuario'] = usuarios['usuario'].astype(str)
            SaldoSku_Usuario = pd.merge(SaldoSku_Usuario, usuarios, on='usuario', how='left')
            SaldoSku_Usuario['usuario'] = SaldoSku_Usuario["usuario"] + '-'+SaldoSku_Usuario["nome"]
            SaldoSku_Usuario.drop('nome', axis=1, inplace=True)
            SaldoSku_Usuario.drop('Endereco', axis=1, inplace=True)

            skus['enderco'] = endereco
            skus['Status Endereco'] = True
            skus['Mensagem'] = f'Endereço {endereco} existe!'
            skus['Status do Saldo']='Cheio'
            SaldoGeral = skus['Saldo Geral'][0]

            detalhatag = pd.read_sql(
                'select codbarrastag, "usuario", "codreduzido" as codreduzido, "DataReposicao"  from "Reposicao".tagsreposicao t '
                'where "Endereco"='+" '"+endereco+"'"'',conn)
            detalhatag = pd.merge(detalhatag, usuarios, on='usuario', how='left')
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
        'select "codreduzido", "engenharia", "Situacao", "usuario", "descricao", "cor", "epc", "numeroop" from "Reposicao"."filareposicaoportag" ce '
        'where "codbarrastag" = %s', (codbarras,))
    codreduzido = pd.DataFrame(cursor.fetchall(), columns=['codreduzido', 'engenharia', 'Situacao', 'usuario',  'descricao', 'cor', 'epc','numeroop'])

    cursor.execute(
        'select count("codbarrastag") as situacao, "codreduzido", "Engenharia", "numeroop", "descricao", "cor", "epc", "tamanho", "totalop","usuario" from "Reposicao"."tagsreposicao" tr '
        'where "codbarrastag" = %s '
        'group by "usuario","codbarrastag", "codreduzido", "Engenharia", "numeroop", "descricao", "cor", "epc", "tamanho", "totalop"', (codbarras,))
    TagApontadas = pd.DataFrame(cursor.fetchall(), columns=['situacao', 'codreduzido', 'Engenharia', 'numeroop', 'descricao', 'cor', 'epc', 'tamanho', 'totalop',"usuario"])

    if not TagApontadas.empty and TagApontadas["situacao"][0] >= 0 and padrao == 0:
        retorno = (
            'Reposto',
            TagApontadas['codreduzido'][0],
            TagApontadas['Engenharia'][0],
            TagApontadas['descricao'][0],
            TagApontadas['cor'][0],
            TagApontadas['epc'][0]
        )
    elif padrao == 1:
        cursor.execute('select "usuario" from "Reposicao"."filareposicaoportag" ce where "numeroop" = %s', (TagApontadas['numeroop'][0],))
        Usuario = pd.DataFrame(cursor.fetchall(), columns=['usuario'])

        if not Usuario.empty:
            retorno = (
                'Reposto',
                TagApontadas['codreduzido'][0],
                TagApontadas['Engenharia'][0],
                TagApontadas['numeroop'][0],
                TagApontadas['descricao'][0],
                TagApontadas['cor'][0],
                TagApontadas['epc'][0],
                TagApontadas['tamanho'][0],
                TagApontadas['totalop'][0],
                Usuario['usuario'][0]
            )
        else:
            retorno = (
                'Reposto',
                TagApontadas['codreduzido'][0],
                TagApontadas['Engenharia'][0],
                TagApontadas['numeroop'][0],
                TagApontadas['descricao'][0],
                TagApontadas['cor'][0],
                TagApontadas['epc'][0],
                TagApontadas['tamanho'][0],
                TagApontadas['totalop'][0],
                "-"
            )
    elif codreduzido.empty:
        retorno = (
            False,
            pd.DataFrame({'Status': [True], 'Mensagem': [f'codbarras {codbarras} encontrado!']}),
            False, False, False, False
        )
    else:
        retorno = (
            codreduzido['codreduzido'][0],
            codreduzido['engenharia'][0],
            codreduzido['usuario'][0],
            codreduzido['descricao'][0],
            codreduzido['cor'][0],
            codreduzido['epc'][0]
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
    reduzido, engenharia, usuario, descricao, cor, epc = Devolver_Inf_Tag(codbarras)
    if reduzido == False:
         return False
    if reduzido == 'Reposto':
        return 'Reposto'
    else:
        #insere os dados da reposicao
        Insert = ' INSERT INTO "Reposicao"."tagsreposicao" ("usuario","codbarrastag","Endereco","DataReposicao","codreduzido","Engenharia","descricao", ' \
                 '"cor", "epc" )' \
                 ' VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s);'
        cursor = conn.cursor()
        cursor.execute(Insert
                       , (usuario, codbarras, endereco,dataHora,reduzido,engenharia,descricao,cor,epc))

        # Obter o número de linhas afetadas
        conn.commit()
        cursor.close()

       # print(f'Apontado a {numeroop} , endereco {endereco}, as {dataHora}')

        return  True
def EstornoApontamento(codbarrastag):
    conn = ConexaoPostgreRailway.conexao()
    situacao, reduzido, engenharia, numeroop, descricao, cor, epc, tam, totalop, usuario = Devolver_Inf_Tag(codbarrastag, 1)
    Insert = 'INSERT INTO  "Reposicao"."filareposicaoportag" ("codreduzido", "engenharia","codbarrastag","numeroop", "descricao", "cor", "epc", "tamanho", "totalop", "Situacao", "usuario") ' \
             'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'+"'Reposição não Iniciada'"+',%s);'
    cursor = conn.cursor()
    cursor.execute(Insert
                   , (reduzido, engenharia, codbarrastag, numeroop, descricao, cor, epc, tam, totalop, usuario))
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


def RetornoLocalCodBarras(codbarras):
    conn = ConexaoPostgreRailway.conexao()
    cursor = conn.cursor()

    # Verificando se está na Fila de Reposição
    cursor.execute(
        'SELECT "codbarrastag" FROM "Reposicao"."filareposicaoportag" ce '
        'WHERE "codbarrastag" = %s', (codbarras,)
    )
    fila_reposicao = pd.DataFrame(cursor.fetchall(), columns=['codbarrastag'])

    if not fila_reposicao.empty:

        retorno = 'A Repor'
    else:
        # Verificando se está na Prateleira
        cursor.execute(
            'SELECT "codbarrastag" FROM "Reposicao"."tagsreposicao" ce '
            'WHERE "codbarrastag" = %s', (codbarras,)
        )
        prateleira = pd.DataFrame(cursor.fetchall(), columns=['codbarrastag'])

        if not prateleira.empty:
            retorno = 'Reposto'
        else:
            retorno = False

    cursor.close()
    conn.close()

    return retorno
# Iniciar o temporizador
inicio = time.time()

# Chamar a função que você deseja medir
print(RetornoLocalCodBarras('01000067443603000512'))

# Parar o temporizador
fim = time.time()

# Calcular o tempo decorrido
tempo_decorrido = fim - inicio

print("Tempo decorrido:", tempo_decorrido, "segundos")
