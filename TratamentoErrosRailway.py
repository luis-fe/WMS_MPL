import ConexaoPostgreRailway
import pandas as pd
import numpy

def VerificarDuplicacoesDeTagsFila():
    conn = ConexaoPostgreRailway.conexao()
    # VERIFICANDO SE EXISTE CODIGO DE BARRAS DUPLICADOS NA FILA
    erro1 = pd.read_sql('select cont from '
                        '(select count(codbarrastag) as cont  from "Reposicao".filareposicaoportag f '
                        'group by codbarrastag) as contagem '
                        'where contagem.cont >1', conn)
    conn.close()
    if erro1.empty:

        return pd.DataFrame({'Mensagem': [f'Nao tem tag duplicada na fila']})
    else:
        return pd.DataFrame({'Mensagem': [f' Atencao  tem tag duplicada na fila!!!']})

def VerificarDuplicacoesTagReposta():
    conn = ConexaoPostgreRailway.conexao()
    # VERIFICANDO SE EXISTE CODIGO DE BARRAS DUPLICADOS NA FILA
    erro1 = pd.read_sql('select cont from '
                        '(select count(codbarrastag) as cont  from "Reposicao".tagsreposicao f '
                        'group by codbarrastag) as contagem '
                        'where contagem.cont >1', conn)
    conn.close()
    if erro1.empty:

        return pd.DataFrame({'Mensagem': [f'Nao tem tag duplicada reposta']})
    else:
        return pd.DataFrame({'Mensagem': [f' Atencao  tem tag duplicada reposta!!!']})

def VerificarDuplicacoesTagRepostaInventario():
    conn = ConexaoPostgreRailway.conexao()
    # VERIFICANDO SE EXISTE CODIGO DE BARRAS DUPLICADOS NA FILA
    erro1 = pd.read_sql('select cont from '
                        '(select count(codbarrastag) as cont  from "Reposicao".tagsreposicao_inventario f '
                        'group by codbarrastag) as contagem '
                        'where contagem.cont >1', conn)
    conn.close()
    if erro1.empty:

        return pd.DataFrame({'Mensagem': [f'Nao tem tag duplicada no inventario']})
    else:
        return pd.DataFrame({'Mensagem': [f' Atencao  tem tag duplicada no inventario!!!']})


def VerificandoTagsSemelhanteFIlaxReposicao():
    conn = ConexaoPostgreRailway.conexao()
    # VERIFICANDO SE EXISTE CODIGO DE BARRAS DUPLICADOS NA FILA
    erro1 = pd.read_sql('select t.codbarrastag  from "Reposicao".tagsreposicao t '
                        'join "Reposicao".filareposicaoportag f on t.codbarrastag = f.codbarrastag  '
                        , conn)
    conn.close()

    if erro1.empty:

        return pd.DataFrame({'Mensagem': [f'Nao tem tag duplicada na fila x reposicao']})
    else:
        erro1['Mensagem'] = 'Tag Duplicada na Fila e no Bipada'
        return erro1

def VerificandoTagsSemelhanteReposicaoxInventario():
    conn = ConexaoPostgreRailway.conexao()
    # VERIFICANDO SE EXISTE CODIGO DE BARRAS DUPLICADOS NA FILA
    erro1 = pd.read_sql('select t.codbarrastag  from "Reposicao".tagsreposicao t '
                        'join "Reposicao".tagsreposicao_inventario f on t.codbarrastag = f.codbarrastag  '
                        , conn)
    conn.close()

    if erro1.empty:

        return pd.DataFrame({'Mensagem': [f'Nao tem tag duplicada na reposicao x inventario']})
    else:
        erro1['Mensagem'] = 'Tag Duplicada na reposicao e no Inventario'
        return erro1

def VerificandoTagsSemelhanteFilaxInventario():
    conn = ConexaoPostgreRailway.conexao()
    # VERIFICANDO SE EXISTE CODIGO DE BARRAS DUPLICADOS NA FILA
    erro1 = pd.read_sql('select t.codbarrastag  from "Reposicao".filareposicaoportag t '
                        'join "Reposicao".tagsreposicao_inventario f on t.codbarrastag = f.codbarrastag  '
                        , conn)
    conn.close()

    if erro1.empty:

        return pd.DataFrame({'Mensagem': [f'Nao tem tag duplicada na fila x inventario']})
    else:
        erro1['Mensagem'] = 'Tag Duplicada na fila e no Inventario'
        return erro1


def ListaErros():
    a = VerificarDuplicacoesDeTagsFila()
    b = VerificarDuplicacoesTagReposta()
    c = VerificarDuplicacoesTagRepostaInventario()
    d = VerificandoTagsSemelhanteFIlaxReposicao()
    e = VerificandoTagsSemelhanteReposicaoxInventario()
    f = VerificandoTagsSemelhanteFilaxInventario()
    df_concat = pd.concat([a, b, c, d,e, f], axis=0)
    return df_concat




def TratandoErroTagsSemelhanteFilaxInventario():

    delete = 'delete from "Reposicao".filareposicaoportag ' \
              ' where codbarrastag in (select t.codbarrastag  from "Reposicao".filareposicaoportag t' \
              ' join "Reposicao".tagsreposicao_inventario ti  on t.codbarrastag = ti.codbarrastag) '


    conn = ConexaoPostgreRailway.conexao()
    cursor = conn.cursor()
    cursor.execute(delete
                   , ( ))

    # Obter o número de linhas afetadas
    numero_linhas_afetadas = cursor.rowcount
    conn.commit()
    cursor.close()
    conn.close()

    return pd.DataFrame({'Mensagem': [f'Limpeza Feita']})


print(ListaErros())