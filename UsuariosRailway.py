import ConexaoPostgreRailway
import pandas as pd
import datetime

def obterHoraAtual():
    agora = datetime.datetime.now()
    hora_str = agora.strftime('%d/%m/%Y %H:%M')
    return hora_str


def PesquisarUsuarios():
    conn = ConexaoPostgreRailway.conexao()
    cursor = conn.cursor()
    cursor.execute('select codigo, nome, funcao, situacao from "Reposicao"."cadusuarios" c')
    usuarios = cursor.fetchall()
    cursor.close()
    conn.close()
    return usuarios
def PesquisarSenha():
    conn = ConexaoPostgreRailway.conexao()
    cursor = conn.cursor()
    cursor.execute('select codigo, nome, senha from "Reposicao"."cadusuarios" c')
    usuarios = cursor.fetchall()
    cursor.close()
    conn.close()
    return usuarios

def PesquisarUsuariosCodigo(codigo):
    conn = ConexaoPostgreRailway.conexao()
    cursor = conn.cursor()
    cursor.execute('select codigo, nome, funcao, situacao from "Reposicao"."cadusuarios" c'
                   ' where codigo = %s',(codigo,))
    usuarios = cursor.fetchall()
    cursor.close()
    conn.close()
    if not usuarios:
        return 0, 0, 0
    else:
        return usuarios[0][1],usuarios[0][2],usuarios[0][3]



def AtualizarInformacoes(novo_nome, nova_funcao, nova_situacao,  codigo):
    conn = ConexaoPostgreRailway.conexao()
    cursor = conn.cursor()
    cursor.execute('UPDATE "Reposicao"."cadusuarios" SET nome=%s, funcao=%s, situacao= %s WHERE codigo=%s',(novo_nome,nova_funcao,nova_situacao, codigo))
    conn.commit()
    cursor.close()
    conn.close()
    return novo_nome

def InserirUsuario(codigo, funcao, nome, senha, situacao):
    conn = ConexaoPostgreRailway.conexao()
    cursor = conn.cursor()
    cursor.execute('INSERT INTO "Reposicao"."cadusuarios" (codigo, funcao, nome, senha, situacao) '
                   'VALUES (%s, %s, %s, %s, %s)',(codigo, funcao, nome, senha, situacao))
    conn.commit()
    cursor.close()
    conn.close()
    return True

def ConsultaUsuarioSenha(codigo, senha):
    conn = ConexaoPostgreRailway.conexao()
    cursor = conn.cursor()
    # Consulta no banco de dados para verificar se o usuário e senha correspondem
    query = 'SELECT COUNT(*) FROM "Reposicao"."cadusuarios" WHERE codigo = %s AND senha = %s'

    cursor.execute(query, (codigo, senha))
    result = cursor.fetchone()[0]
    cursor.close()


    return result

def RegistroLog(codigo):
    conn = ConexaoPostgreRailway.conexao()
    # registrando data e hora do log
    hora = obterHoraAtual()
    cursor = conn.cursor()
    log = 'insert into "Reposicao"."horariodolog" ( "codigo", "datalog" ) ' \
          'values(%s, %s)'

    cursor.execute(log, (codigo, hora))
    conn.commit()
    cursor.close()
    conn.close()
    return True