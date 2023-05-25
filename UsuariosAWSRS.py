import ConecaoAWSRS
import pandas as pd



def PesquisarUsuarios():
    conn = ConecaoAWSRS.conexao()
    cursor = conn.cursor()
    cursor.execute('select codigo, nome, funcao, situacao from "Reposicao"."cadusuarios" c')
    usuarios = cursor.fetchall()
    cursor.close()
    conn.close()
    return usuarios

def PesquisarUsuariosCodigo(codigo):
    conn = ConecaoAWSRS.conexao()
    cursor = conn.cursor()
    cursor.execute('select codigo, nome, funcao, situacao from "Reposicao"."cadusuarios" c'
                   ' where codigo = %s',(codigo,))
    usuarios = cursor.fetchall()
    cursor.close()
    conn.close()
    return usuarios[0][1],usuarios[0][2],usuarios[0][3]



def AtualizarInformacoes(novo_nome, nova_funcao, nova_situacao,  codigo):
    conn = ConecaoAWSRS.conexao()
    cursor = conn.cursor()
    cursor.execute('UPDATE "Reposicao"."cadusuarios" SET nome=%s, funcao=%s, situacao= %s WHERE codigo=%s',(novo_nome,nova_funcao,nova_situacao, codigo))
    conn.commit()
    cursor.close()
    conn.close()
    return novo_nome

def InserirUsuario(novo_nome, nova_funcao, nova_situacao, senha, codigo):
    conn = ConecaoAWSRS.conexao()
    cursor = conn.cursor()
    cursor.execute('INSERT INTO "Reposicao"."cadusuarios" (codigo, funcao, nome, senha, situacao) '
                   'VALUES (%s, %s, %s, %s, %s)',(novo_nome,nova_funcao,nova_situacao, senha, codigo))
    conn.commit()
    cursor.close()
    conn.close()
    return True

def ConsultaUsuarioSenha(codigo, senha):
    conn = ConecaoAWSRS.conexao()
    cursor = conn.cursor()
    # Consulta no banco de dados para verificar se o usuário e senha correspondem
    query = 'SELECT COUNT(*) FROM "Reposicao"."cadusuarios" WHERE codigo = %s AND senha = %s'
    cursor.execute(query, (codigo, senha))
    result = cursor.fetchone()[0]

    return result
