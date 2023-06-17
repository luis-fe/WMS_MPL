import ConexaoPostgreRailway
import pandas as pd
import datetime
import numpy
def obterHoraAtual():
    agora = datetime.datetime.now()
    hora_str = agora.strftime('%Y-%m-%d %H:%M:%S')
    return hora_str
def RegistrarInventario(usuario, data, endereco):
    conn = ConexaoPostgreRailway.conexao()
    # VERIFICANDO SE EXISTE CODIGO DE BARRAS DUPLICADOS NA FILA
    inserir = 'insert into "Reposicao".registroinventario ("usuario","data","endereco")  '\
                        ' values(%s, %s, %s) '
    cursor = conn.cursor()
    cursor.execute(inserir
                   , (
                   usuario, data, endereco))

    # Obter o número de linhas afetadas
    numero_linhas_afetadas = cursor.rowcount
    conn.commit()
    cursor.close()
    conn.close()
    return True

def ApontarTagInventario(codbarra, endereco, usuario, padrao=False):
    conn = ConexaoPostgreRailway.conexao()

    validador, colu1, colu_epc, colu_tamanho, colu_cor, colu_eng, colu_red, colu_desc, colu_numeroop, colu_totalop   = PesquisarTagPrateleira(codbarra)

    if validador == 1:
        query = 'update "Reposicao".tagsreposicao_inventario '\
            'set situacaoinventario  = '+"'OK'"+ \
            'where codbarrastag = %s'
        cursor = conn.cursor()
        cursor.execute(query
                       , (
                           codbarra,))

        # Obter o número de linhas afetadas
        numero_linhas_afetadas = cursor.rowcount
        conn.commit()
        cursor.close()
        conn.close()
        return pd.DataFrame({'Status Conferencia': [True], 'Mensagem': [f'tag: {codbarra} conferida!']})
    if validador == False:
        conn.close()
        return pd.DataFrame({'Status Conferencia': [False], 'Mensagem': [f'tag: {codbarra} não exite no estoque! ']})
    if validador ==3:
        query = 'insert into  "Reposicao".tagsreposicao_inventario ' \
                '("codbarrastag","Endereco","situacaoinventario","epc","tamanho","cor","Engenharia","CodReduzido","descricao","numeroop","totalop","Usuario") ' \
                'values(%s,%s,'+"'adicionado do fila'"+',%s,%s,%s,%s,%s,%s,%s,%s,%s)'
        cursor = conn.cursor()
        cursor.execute(query
                       , (
                           codbarra,endereco,colu_epc, colu_tamanho,colu_cor,colu_eng,colu_red,colu_desc,colu_numeroop,colu_totalop, usuario))

        # Obter o número de linhas afetadas
        numero_linhas_afetadas = cursor.rowcount
        conn.commit()
        cursor.close()
        delete = 'Delete from "Reposicao"."filareposicaoportag"  ' \
                 'where "codbarrastag" = %s;'
        cursor = conn.cursor()
        cursor.execute(delete
                       , (
                           codbarra,))
        conn.commit()
        cursor.close()


        conn.close()
        return pd.DataFrame({'Status Conferencia': [True], 'Mensagem': [f'tag: {codbarra} veio da FilaReposicao, será listado ao salvar ']})
    if validador == 2 and padrao == False:

        return pd.DataFrame({'Status Conferencia': [False],
                             'Mensagem': [f'tag: {codbarra} veio de outro endereço: {colu1} , deseja prosseguir?']})
    if validador == 2 and padrao == True:
        insert = 'INSERT INTO "Reposicao".tagsreposicao_inventario ("Usuario", "codbarrastag", "CodReduzido", "Endereco", ' \
                 '"Engenharia", "DataReposicao", "descricao", "epc", "StatusEndereco", ' \
                 '"numeroop", "cor", "tamanho", "totalop", "situacaoinventario") ' \
                 'SELECT "Usuario", "codbarrastag", "CodReduzido", %s, "Engenharia", ' \
                 '"DataReposicao", "descricao", "epc", "StatusEndereco", "numeroop", "cor", "tamanho", "totalop", ' \
                 "'endereco migrado'" \
                 'FROM "Reposicao".tagsreposicao t ' \
                 'WHERE "codbarrastag" = %s;'
        cursor = conn.cursor()
        cursor.execute(insert, (endereco, codbarra))
        conn.commit()
        cursor.close()
        delete = 'Delete from "Reposicao"."tagsreposicao"  ' \
                 'where "codbarrastag" = %s;'
        cursor = conn.cursor()
        cursor.execute(delete
                       , (
                           codbarra,))
        conn.commit()
        cursor.close()
        conn.close()
        return pd.DataFrame({'Status Conferencia': [True], 'Mensagem': [f'tag: {codbarra} veio de outro endereço, será listadado ao salvar']})
    else:
        return pd.DataFrame({'Status Conferencia': [False], 'Mensagem': [f'tag: {codbarra} não exite no estoque! ']})



def PesquisarTagPrateleira(codbarra):
    conn = ConexaoPostgreRailway.conexao()
    query1 = pd.read_sql('SELECT "codbarrastag" from "Reposicao".tagsreposicao_inventario t '
            'where codbarrastag = '+"'"+codbarra+"'",conn )

    if not query1.empty:
        conn.close()
        return 1, 2, 3, 4, 5, 6 ,7 ,8 , 9 , 10

    else:
        query2 = pd.read_sql('select codbarrastag, "Endereco"   from "Reposicao".tagsreposicao f  '
                             'where codbarrastag = ' + "'" + codbarra + "'", conn)
        if not query2.empty:

            conn.close()
            return 2, query2["Endereco"][0], 2, 2,2,2,2,2,2,2
        else:
            query3 = pd.read_sql('select "codbarrastag","epc", "tamanho", "cor", "CodEngenharia" , "codReduzido",  '
                                 '"descricao" ,"numeroop", "totalop" from "Reposicao".filareposicaoportag f  '
                                 'where codbarrastag = ' + "'" + codbarra + "'", conn)

            if not query3.empty:
                conn.close()
                return 3, query3["codbarrastag"][0],query3["epc"][0],query3["tamanho"][0],query3["cor"][0],query3["CodEngenharia"][0],query3["codReduzido"][0], \
                    query3["descricao"][0],query3["numeroop"][0],query3["totalop"][0]

            else:
                query4 = pd.read_sql('select "Endereco"  from "Reposicao".tagsreposicao t  '
                                     'where codbarrastag = ' + "'" + codbarra + "'", conn)
                if not query3.empty:
                    conn.close()
                    return query4["Endereco"][0], 4, 4, 4,4,4,4,4,4,4
                else:
                    conn.close()
                    return False, False, False, False, False, False, False, False, False, False
def SituacaoEndereco(endereco,usuario, data):
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
            RegistrarInventario(usuario, data, endereco)
            return pd.DataFrame({'Status Endereco': [True], 'Mensagem': [f'endereco {endereco} existe!'],
                                 'Status do Saldo': ['Vazio, preparado para o INVENTARIO !']})
        else:
            skus = pd.read_sql('select "CodReduzido", count(codbarrastag)as Saldo  from "Reposicao".tagsreposicao t  '
                                    'where "Endereco"='+" '"+endereco+"'"+' group by "Endereco" , "CodReduzido" ',conn)

            skus['enderco'] = endereco
            skus['Status Endereco'] = True
            skus['Mensagem'] = f'Endereço {endereco} existe!'
            skus['Status do Saldo']='Cheio, será esvaziado para o INVENTARIO'

            DetalhaSku =pd.read_sql('select "CodReduzido", "codbarrastag" ,"epc"  from "Reposicao".tagsreposicao t  '
                                    'where "Endereco"='+" '"+endereco+"'",conn)

            conn.close()
            RegistrarInventario(usuario, data, endereco)

            data = {
                '2 - Endereco': f'{skus["enderco"][0]} ',
                '3 - Status Endereco': f'{skus["Status Endereco"][0]} ',
                '1 - Mensagem': f'{skus["Mensagem"][0]} ',
                '4- Suituacao':f'{skus["Status do Saldo"][0]} ',
                '5- Detalhamento dos Tags:':DetalhaSku.to_dict(orient='records')
            }
            return [data]


def Estoque_endereco(endereco):
    conn = ConexaoPostgreRailway.conexao()
    consultaSql = 'select count(codbarrastag)as Saldo  from "Reposicao".tagsreposicao t  ' \
                  'where "Endereco" = %s '\
                    'group by "Endereco" '

    cursor = conn.cursor()
    cursor.execute(consultaSql, (endereco,))
    resultado = cursor.fetchall()
    cursor.close()
    if not resultado:
        return 0
    else:
        return resultado[0][0]

def SalvarInventario(endereco):
    conn = ConexaoPostgreRailway.conexao()

    # Inserir de volta as tags que deram certo
    insert = 'INSERT INTO "Reposicao".tagsreposicao ("Usuario", "codbarrastag", "CodReduzido", "Endereco", ' \
             '"Engenharia", "DataReposicao", "descricao", "epc", "StatusEndereco", ' \
             '"numeroop", "cor", "tamanho", "totalop") ' \
             'SELECT "Usuario", "codbarrastag", "CodReduzido", "Endereco", "Engenharia", ' \
             '"DataReposicao", "descricao", "epc", "StatusEndereco", "numeroop", "cor", "tamanho", "totalop" ' \
             'FROM "Reposicao".tagsreposicao_inventario t ' \
             'WHERE "Endereco" = %s and "situacaoinventario" = %s ;'
    cursor = conn.cursor()
    cursor.execute(insert, (endereco,'OK'))
    numero_linhas_afetadas = cursor.rowcount
    conn.commit()
    cursor.close()

    #deletar as tag's ok

    delete = 'Delete FROM "Reposicao".tagsreposicao_inventario t ' \
             'WHERE "Endereco" = %s and "situacaoinventario" = %s;'
    cursor = conn.cursor()
    cursor.execute(delete, (endereco,'OK'))
    conn.commit()
    cursor.close()


    # Avisar sobre as Tags migradas
    Aviso = pd.read_sql('SELECT * FROM "Reposicao".tagsreposicao_inventario t '
             'WHERE "Endereco" = '+ "'"+endereco+"'"+' and "situacaoinventario" is not null ;', conn)

    #Autorizar migracao
    numero_tagsMigradas = Aviso["Endereco"].size
    datahora = obterHoraAtual()
    insert = 'INSERT INTO "Reposicao".tagsreposicao ("Usuario", "codbarrastag", "CodReduzido", "Endereco", ' \
             '"Engenharia", "DataReposicao", "descricao", "epc", "StatusEndereco", ' \
             '"numeroop", "cor", "tamanho", "totalop") ' \
             'SELECT "Usuario", "codbarrastag", "CodReduzido", "Endereco", "Engenharia", ' \
             '%s , "descricao", "epc", "StatusEndereco", "numeroop", "cor", "tamanho", "totalop" ' \
             'FROM "Reposicao".tagsreposicao_inventario t ' \
             'WHERE "Endereco" = %s and "situacaoinventario" is not null ;'
    cursor = conn.cursor()
    cursor.execute(insert, (datahora, endereco))
    numero_linhas_afetadas = cursor.rowcount
    conn.commit()
    cursor.close()

    # deletar as tag's MIGRADAS

    deleteMigradas = 'Delete FROM "Reposicao".tagsreposicao_inventario t ' \
             'WHERE "Endereco" = %s and "situacaoinventario" is not null ;'
    cursor = conn.cursor()
    cursor.execute(deleteMigradas, (endereco,))
    conn.commit()
    cursor.close()

    # Tags nao encontradas , avisar e trazer a lista de codigo barras e epc para o usuario tomar decisao
    Aviso2 = pd.read_sql('SELECT "codbarrastag", "epc" FROM "Reposicao".tagsreposicao_inventario t '
                         'WHERE "Endereco" = ' + "'" + endereco + "'" + ' and "situacaoinventario" is null;', conn)

    numero_tagsNaoEncontradas = Aviso2["codbarrastag"].size

    data = {
        '1 - Tags Encontradas': f'{numero_linhas_afetadas} foram encontradas e inventariadas com sucesso',
        '2 - Tags Migradas de endereço': 
            f'{numero_tagsMigradas} foram migradas para o endereço {endereco} e inventariadas com sucesso',
        '3 - Tags Nao encontradas': f'{numero_tagsNaoEncontradas} não foram encontradas no endereço {endereco}',
        '3.1 - Listagem Tags Nao encontradas [Codigo Barras, EPC]': Aviso2.to_dict(orient='records')
    }

    return [data]
