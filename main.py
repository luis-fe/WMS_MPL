
from flask import Flask, render_template, jsonify, request
import psycopg2
import os
from apscheduler.schedulers.background import BackgroundScheduler
from functools import wraps

app = Flask(__name__)

port = int(os.environ.get('PORT', 5000))

db_name = "wms_bd"  
db_user = "wms"
db_password = "Master100"
db_host = "wmsbd.cyiuowfro4wv.sa-east-1.rds.amazonaws.com"
portbanco = "5432"

conn = psycopg2.connect(database=db_name, user=db_user, password=db_password, host=db_host, port=portbanco)
cursor = conn.cursor()
# Decorator para verificar o token fixo
def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')

        if token == 'a40016aabcx9':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)

        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function


@app.route('/')
def home():
    return render_template('index.html')

@app.route('/api/Filaeposicao', methods=['GET'])
def get_filaeposicao():
    cursor.execute('select * from "Reposicao"."FilaReposicaoTags" frt ')
    filaeposicao = cursor.fetchall()
    return jsonify(filaeposicao)

# Rota protegida que requer o token fixo
@app.route('/api/Usuarios', methods=['GET'])
@token_required
def get_usuarios():
    cursor.execute('select * from "Reposicao"."cadusuarios" c')
    usuarios = cursor.fetchall()

    # Obtém os nomes das colunas
    column_names = [desc[0] for desc in cursor.description]

    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    usuarios_data = []
    for row in usuarios:
        usuario_dict = {}
        for i, value in enumerate(row):
            usuario_dict[column_names[i]] = value
        usuarios_data.append(usuario_dict)

    return jsonify(usuarios_data)
# Rota para atualizar um usuário pelo código
@app.route('/api/Usuarios/<int:codigo>', methods=['PUT'])
@token_required
def update_usuario(codigo):
    # Obtém os dados do corpo da requisição (JSON)
    data = request.get_json()

    # Verifica se a coluna "funcao" está presente nos dados recebidos
    if 'funcao' in data:
        # Obtém o valor da coluna "funcao"
        nova_funcao = data['funcao']
        cursor.execute('UPDATE "Reposicao"."cadusuarios" SET funcao=%s WHERE codigo=%s', (nova_funcao, codigo))
        conn.commit()

        # Retorna uma resposta de sucesso
        return jsonify({'message': 'Usuário atualizado com sucesso'})

    # Retorna uma resposta de erro se a coluna "funcao" não estiver presente nos dados
    return jsonify({'message': 'Coluna "funcao" não encontrada nos dados'}), 400


@app.route('/api/Usuarios', methods=['POST'])
@token_required
def criar_usuario():
    # Obtenha os dados do corpo da requisição
    novo_usuario = request.get_json()

    # Extraia os valores dos campos do novo usuário
    codigo = novo_usuario.get('codigo')
    funcao = novo_usuario.get('funcao')
    nome = novo_usuario.get('nome')
    senha = novo_usuario.get('senha')
    situacao = novo_usuario.get('situacao')

    # Faça a lógica para inserir o novo usuário no banco de dados
    # (substitua com sua lógica de banco de dados)

    # Exemplo: Inserindo o novo usuário em uma tabela "Usuarios"
    # usando SQL
    cursor.execute(
        'INSERT INTO "Reposicao"."cadusuarios" (codigo, funcao, nome, senha, situacao) '
        'VALUES (%s, %s, %s, %s, %s)',
        (codigo, funcao, nome, senha, situacao)
    )
    conn.commit()

    # Retorne uma resposta indicando o sucesso da operação
    return jsonify({'message': 'Novo usuário criado com sucesso'}), 201

# Implementação da API para verificar usuário e senha
@app.route('/api/UsuarioSenha', methods=['POST'])
@token_required
def check_user_password():
    # Obtém os dados do corpo da requisição (JSON)
    data = request.json

    # Extrai o código do usuário e a senha dos dados recebidos
    codigo = data.get('codigo')
    senha = data.get('senha')

    # Verifica se o código do usuário e a senha foram fornecidos
    if codigo is None or senha is None:
        return jsonify({'message': 'Código do usuário e senha devem ser fornecidos.'}), 400

    # Código de conexão com o banco de dados
    # ...

    # Consulta no banco de dados para verificar se o usuário e senha correspondem
    query = 'SELECT COUNT(*) FROM "Reposicao"."cadusuarios"  WHERE codigo = %s AND senha = %s'
    cursor.execute(query, (codigo, senha))
    result = cursor.fetchone()[0]

    # Verifica se o usuário existe e retorna a mensagem correspondente
    if result == 1:
        return jsonify({'message': f'Usuário {codigo} existe!'})
    else:
        return jsonify({False})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=port)
    cursor.close()
    conn.close()
