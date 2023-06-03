import ConexaoPostgreRailway
import pandas as pd
import numpy

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

def ApontarTagInventario(codbarra, endereco, usuario):
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
                '("codbarrastag","Endereco","situacaoinventario","Epc","tamanho","cor","Engenharia","CodReduzido","Descricao","numeroop","totalop","Usuario") ' \
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
    if validador == 2:
        insert = 'INSERT INTO "Reposicao".tagsreposicao_inventario ("Usuario", "codbarrastag", "CodReduzido", "Endereco", ' \
                 '"Engenharia", "DataReposicao", "Descricao", "Epc", "StatusEndereco", ' \
                 '"numeroop", "cor", "tamanho", "totalop", "situacaoinventario") ' \
                 'SELECT "Usuario", "codbarrastag", "CodReduzido", %s, "Engenharia", ' \
                 '"DataReposicao", "Descricao", "Epc", "StatusEndereco", "numeroop", "cor", "tamanho", "totalop", ' \
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
        query2 = pd.read_sql('select codbarrastag  from "Reposicao".tagsreposicao f  '
                             'where codbarrastag = ' + "'" + codbarra + "'", conn)
        if not query2.empty:

            conn.close()
            return 2, 2, 2, 2,2,2,2,2,2,2
        else:
            query3 = pd.read_sql('select "codbarrastag","epc", "tamanho", "Cor", "CodEngenharia" , "codReduzido",  '
                                 '"descricao" ,"numeroop", "totalop" from "Reposicao".filareposicaoportag f  '
                                 'where codbarrastag = ' + "'" + codbarra + "'", conn)

            if not query3.empty:
                conn.close()
                return 3, query3["codbarrastag"][0],query3["epc"][0],query3["tamanho"][0],query3["Cor"][0],query3["CodEngenharia"][0],query3["codReduzido"][0], \
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
            skus = pd.read_sql('select  "codreduzido", "Saldo"  from "Reposicao"."estoque" e '
                                    'where "endereco"='+" '"+endereco+"'",conn)
            conn.close()
            skus['enderco'] = endereco
            skus['Status Endereco'] = True
            skus['Mensagem'] = f'Endereço {endereco} existe!'
            skus['Status do Saldo']='Cheio, será esvaziado para o INVENTARIO'
            RegistrarInventario(usuario, data, endereco)
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

def SalvarInventario(endereco):
    conn = ConexaoPostgreRailway.conexao()

    # Inserir de volta as tags que deram certo
    insert = 'INSERT INTO "Reposicao".tagsreposicao ("Usuario", "codbarrastag", "CodReduzido", "Endereco", ' \
             '"Engenharia", "DataReposicao", "Descricao", "Epc", "StatusEndereco", ' \
             '"numeroop", "cor", "tamanho", "totalop") ' \
             'SELECT "Usuario", "codbarrastag", "CodReduzido", "Endereco", "Engenharia", ' \
             '"DataReposicao", "Descricao", "Epc", "StatusEndereco", "numeroop", "cor", "tamanho", "totalop"' \
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

    # deletar as tag's MIGRADAS

    deleteMigradas = 'Delete FROM "Reposicao".tagsreposicao_inventario t ' \
             'WHERE "Endereco" = %s and "situacaoinventario" is not null ;'
    cursor = conn.cursor()
    cursor.execute(deleteMigradas, (endereco,))
    conn.commit()
    cursor.close()

    # Tags nao encontradas , avisar e trazer a lista de codigo barras e epc para o usuario tomar decisao
    Aviso2 = pd.read_sql('SELECT "codbarrastag" , "Epc" FROM "Reposicao".tagsreposicao_inventario t '
             'WHERE "Endereco" = '+ "'"+endereco+"'"+' and "situacaoinventario" is  null ;', conn)

    numero_tagsNaoEncontradas = Aviso2["codbarrastag"].size

    return pd.DataFrame({
        '1 - Tags Encontradas': [f'{numero_linhas_afetadas} foram encontradas e inventariadas com sucesso'],
        '2 - Tags Migradas de endereço': [
            f'{numero_tagsMigradas} foram migradas para o endereço {endereco} e inventariadas com sucesso'],
        '3 - Tags Nao encontradas': [f'{numero_tagsNaoEncontradas} não foram encontradas no endereço {endereco}'],
        '3.1 - Listagem Tags Nao encontradas': pd.Series({'codigo Barras/EPC': [f'{Aviso2}']})
    })