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

        return pd.DataFrame({'Mensagem': [f'Nao tem Tag duplicada na Fila de Reposicao']})
    else:
        return pd.DataFrame({'Mensagem': [f' Atencao, há  Tags duplicada na Fila de Reposicao!!!']})

def VerificarDuplicacoesTagReposta():
    conn = ConexaoPostgreRailway.conexao()
    # VERIFICANDO SE EXISTE CODIGO DE BARRAS DUPLICADOS NA FILA
    erro1 = pd.read_sql('select cont from '
                        '(select count(codbarrastag) as cont  from "Reposicao".tagsreposicao f '
                        'group by codbarrastag) as contagem '
                        'where contagem.cont >1', conn)
    conn.close()
    if erro1.empty:

        return pd.DataFrame({'Mensagem': [f'Nao tem Tag duplicada nas Prateleiras (endereço)']})
    else:
        return pd.DataFrame({'Mensagem': [f' Atencao,  tem Tag nas Prateleiras (endereço)!!!']})

def VerificarDuplicacoesTagRepostaInventario():
    conn = ConexaoPostgreRailway.conexao()
    # VERIFICANDO SE EXISTE CODIGO DE BARRAS DUPLICADOS NA FILA
    erro1 = pd.read_sql('select cont from '
                        '(select count(codbarrastag) as cont  from "Reposicao".tagsreposicao_inventario f '
                        'group by codbarrastag) as contagem '
                        'where contagem.cont >1', conn)
    conn.close()
    if erro1.empty:

        return pd.DataFrame({'Mensagem': [f'Nao tem Tag duplicada no Inventario']})
    else:
        return pd.DataFrame({'Mensagem': [f' Atencao,  tem Tag duplicada no Inventario!!!']})


def VerificandoTagsSemelhanteFIlaxReposicao():
    conn = ConexaoPostgreRailway.conexao()
    # VERIFICANDO SE EXISTE CODIGO DE BARRAS DUPLICADOS NA FILA
    erro1 = pd.read_sql('select t.codbarrastag  from "Reposicao".tagsreposicao t '
                        'join "Reposicao".filareposicaoportag f on t.codbarrastag = f.codbarrastag  '
                        , conn)
    conn.close()

    if erro1.empty:

        return pd.DataFrame({'Mensagem': [f'Nao tem tag duplicada na Fila x Pratleiras']})
    else:
        erro1['Mensagem'] = 'Atencao, Tag Duplicada na Fila e no Bipada'
        return erro1

def VerificandoTagsSemelhanteReposicaoxInventario():
    conn = ConexaoPostgreRailway.conexao()
    # VERIFICANDO SE EXISTE CODIGO DE BARRAS DUPLICADOS NA FILA
    erro1 = pd.read_sql('select t.codbarrastag  from "Reposicao".tagsreposicao t '
                        'join "Reposicao".tagsreposicao_inventario f on t.codbarrastag = f.codbarrastag  '
                        , conn)
    conn.close()

    if erro1.empty:

        return pd.DataFrame({'Mensagem': [f'Nao tem tag duplicada na Prateleira  x inventario']})
    else:
        erro1['Mensagem'] = 'Tag Duplicada na Prateleira e no Inventario'
        return erro1

def VerificandoTagsSemelhanteFilaxInventario():
    conn = ConexaoPostgreRailway.conexao()
    # VERIFICANDO SE EXISTE CODIGO DE BARRAS DUPLICADOS NA FILA
    erro1 = pd.read_sql('select t.codbarrastag  from "Reposicao".filareposicaoportag t '
                        'join "Reposicao".tagsreposicao_inventario f on t.codbarrastag = f.codbarrastag  '
                        , conn)
    conn.close()

    if erro1.empty:

        return pd.DataFrame({'Mensagem': [f'Nao tem Tag duplicada na Fila Reposicao x inventario']})
    else:
        erro1['Mensagem'] = 'Tag Duplicada na Fila Reposicao e no Inventario'
        return erro1
def VerificarDuplicacoesAtribuicaoUsuarioOP():
    conn = ConexaoPostgreRailway.conexao()
    # VERIFICANDO SE EXISTE CODIGO DE BARRAS DUPLICADOS NA FILA
    erro1 = pd.read_sql('select op from ( '
                        'select op , count(ocorrencia) as ocorrencia from ( '
                        'select numeroop as op , "Usuario" as usu, numeroop as ocorrencia from "Reposicao".filareposicaoportag f '
                        'group by numeroop , "Usuario") as filadupla '
                        'group by op) as teste '
                        'where teste.ocorrencia > 1', conn)
    conn.close()
    if erro1.empty:

        return pd.DataFrame({'Mensagem': [f'Nao tem dados duplicados de Usuario Atribuido na Fila Reposicao']})
    else:
        erro1['Mensagem'] = f' Tem dados duplicados de Usuario Atribuido Na Fila Reposicao!!!'
        return erro1

def ListaErros():
    a = VerificarDuplicacoesDeTagsFila()
    b = VerificarDuplicacoesTagReposta()
    c = VerificarDuplicacoesTagRepostaInventario()
    d = VerificandoTagsSemelhanteFIlaxReposicao()
    e = VerificandoTagsSemelhanteReposicaoxInventario()
    f = VerificandoTagsSemelhanteFilaxInventario()
    g = VerificarDuplicacoesAtribuicaoUsuarioOP()
    df_concat = pd.concat([a, b, c, d,e, f,g], axis=0)
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
def TratandoErroTagsSemelhanteFilaxReposicao():

    delete = 'delete from "Reposicao".filareposicaoportag ' \
              ' where codbarrastag in (select t.codbarrastag  from "Reposicao".filareposicaoportag t' \
              ' join "Reposicao".tagsreposicao ti  on t.codbarrastag = ti.codbarrastag) '


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

def TratandoErroTagsSemelhanteInventarioxReposicao():

    delete = 'delete from "Reposicao".tagsreposicao_inventario ' \
              ' where codbarrastag in (select t.codbarrastag  from "Reposicao".tagsreposicao_inventario t' \
              ' join "Reposicao".tagsreposicao ti  on t.codbarrastag = ti.codbarrastag) '


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

