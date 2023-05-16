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

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=port)
    cursor.close()
    conn.close()
