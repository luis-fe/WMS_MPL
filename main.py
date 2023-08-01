from flask import Flask, render_template, jsonify, request
import pandas as pd
import os
from functools import wraps
import DetalhaPedido
import DistribuicaoPedidosRailway
import Incremento
import InventarioPrateleira
import PediosReporRailway
import Relatorios
import Silk_PesquisaNew
import TratamentoErrosRailway
import UsuariosRailway
import OPfilaReporRailway
import ReposicaoRailway
import ReposicaoSkuRailway
#TESTE

app = Flask(__name__)
port = int(os.environ.get('PORT', 5000))




# Decorator para verificar o token fixo
def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a40016aabcx9':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401
    return decorated_function
# Rota pagina inicial
@app.route('/')
def home():
    return render_template('index.html')
# Rota protegida que requer o token fixo para trazer os Usuarios Cadastrados
@app.route('/api/Usuarios', methods=['GET'])
@token_required
def get_usuarios():
    usuarios = UsuariosRailway.PesquisarUsuarios()
    # Obtém os nomes das colunas
    column_names = ['codigo', 'nome', 'funcao', 'situacao']
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    usuarios_data = []
    for row in usuarios:
        usuario_dict = dict(zip(column_names, row))
        usuarios_data.append(usuario_dict)
    return jsonify(usuarios_data)
@app.route('/api/UsuarioSenhaRestricao', methods=['GET'])
@token_required
def get_usuariosRestricao():
    usuarios = UsuariosRailway.PesquisarSenha()

    # Obtém os nomes das colunas
    column_names = ['codigo', 'nome ','senha']

    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    usuarios_data = []
    for row in usuarios:
        usuario_dict = dict(zip(column_names, row))
        usuarios_data.append(usuario_dict)

    return jsonify(usuarios_data)

@app.route('/api/UsuarioSenhaRestricaoSEMTOKEN', methods=['GET'])
def get_usuariosRestricaoSEMTOKEN():
    # Obtém o código do usuário e a senha dos parâmetros da URL
    codigo = request.args.get('codigo')
    senha = request.args.get('senha')

    # Verifica se o código do usuário e a senha foram fornecidos
    if codigo is None or senha is None:
        return jsonify({'message': 'Código do usuário e senha devem ser fornecidos.'}), 400

    # Consulta no banco de dados para verificar se o usuário e senha correspondem
    result = UsuariosRailway.ConsultaUsuarioSenha(codigo, senha)

    # Verifica se o usuário existe
    if result == 1:
        # Consulta no banco de dados para obter informações adicionais do usuário

        nome, funcao, situacao = UsuariosRailway.PesquisarUsuariosCodigo(codigo)

        # Verifica se foram encontradas informações adicionais do usuário
        if nome != 0:
            UsuariosRailway.RegistroLog(codigo)
            # Retorna as informações adicionais do usuário
            return jsonify({
                "status": True,
                "message": "Usuário e senha VALIDADOS!",
                "nome": nome,
                "funcao": funcao,
                "situacao": situacao
            })
        else:
            return jsonify({'message': 'Não foi possível obter informações adicionais do usuário.'}), 500
    else:
        return jsonify({"status": False,
                        "message": 'Usuário ou senha não existe'}), 401


# Rota para atualizar um usuário pelo código
@app.route('/api/Usuarios/<int:codigo>', methods=['POST'])
@token_required
def update_usuario(codigo):
    # Obtém os dados do corpo da requisição (JSON)
    data = request.get_json()
    # Verifica se a coluna "funcao" está presente nos dados recebidos
    nome_ant, funcao_ant, situacao_ant = UsuariosRailway.PesquisarUsuariosCodigo(codigo)
    if 'funcao' in data:
        nova_funcao = data['funcao']
    else:
        nova_funcao = funcao_ant
    if 'nome' in data:
        nome_novo = data['nome']
    else:
        nome_novo = nome_ant
    if 'situacao' in data:
        situacao_novo = data['situacao']
    else:
        situacao_novo = situacao_ant
    UsuariosRailway.AtualizarInformacoes(nome_novo, nova_funcao, situacao_novo, codigo)

    return jsonify({'message': f'Dados do Usuário {codigo} - {nome_novo} atualizado com sucesso'})


@app.route('/api/Usuarios', methods=['PUT'])
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
    # inserir o novo usuário no banco de dados
    c, n, f = UsuariosRailway.PesquisarUsuariosCodigo(codigo)
    if c != 0:
        return jsonify({'message': f'Novo usuário:{codigo}- {nome} ja existe'}), 201
    else:
        UsuariosRailway.InserirUsuario(codigo, funcao, nome, senha, situacao)
        # Retorne uma resposta indicando o sucesso da operação
        return jsonify({'message': f'Novo usuário:{codigo}- {nome} criado com sucesso'}), 201

#Rota com parametros para check do Usuario e Senha
@app.route('/api/UsuarioSenha', methods=['GET'])
@token_required
def check_user_password():
    # Obtém o código do usuário e a senha dos parâmetros da URL
    codigo = request.args.get('codigo')
    senha = request.args.get('senha')

    # Verifica se o código do usuário e a senha foram fornecidos
    if codigo is None or senha is None:
        return jsonify({'message': 'Código do usuário e senha devem ser fornecidos.'}), 400

    # Consulta no banco de dados para verificar se o usuário e senha correspondem
    result = UsuariosRailway.ConsultaUsuarioSenha(codigo, senha)

    # Verifica se o usuário existe
    if result == 1:
        # Consulta no banco de dados para obter informações adicionais do usuário

        nome, funcao, situacao = UsuariosRailway.PesquisarUsuariosCodigo(codigo)

        # Verifica se foram encontradas informações adicionais do usuário
        if nome != 0:
            UsuariosRailway.RegistroLog(codigo)
            # Retorna as informações adicionais do usuário
            return jsonify({
                "status": True,
                "message": "Usuário e senha VALIDADOS!",
                "nome": nome,
                "funcao": funcao,
                "situacao": situacao
            })
        else:
            return jsonify({'message': 'Não foi possível obter informações adicionais do usuário.'}), 500
    else:
        return jsonify({"status": False,
            "message":'Usuário ou senha não existe'}), 401

@app.route('/api/TagsReposicao/Resumo', methods=['GET'])
@token_required
def get_TagsReposicao():
    TagReposicao = OPfilaReporRailway.ProdutividadeRepositores()

     # Obtém os nomes das colunas
    column_names = ['usuario', 'Qtde', 'DataReposicao', 'min', 'max']
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    TagReposicao_data = []
    for row in TagReposicao:
        # Converte a coluna 'Qtde' para inteiro
        row = list(row)  # Convertendo a tupla em uma lista mutável
        row[1] = int(row[1])  # Convertendo o valor da coluna 'Qtde' para inteiro
        TagReposicao_dict = dict(zip(column_names, row))
        TagReposicao_data.append(TagReposicao_dict)

    return jsonify(TagReposicao_data)


@app.route('/api/TagsSeparacao/Resumo', methods=['GET'])
@token_required
def get_TagsSeparacao():
    TagReposicao = OPfilaReporRailway.ProdutividadeSeparadores()

    # Obtém os nomes das colunas
    column_names = TagReposicao.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    pedidos_data = []
    for index, row in TagReposicao.iterrows():
        pedidos_dict = {}
        for column_name in column_names:
            pedidos_dict[column_name] = row[column_name]
        pedidos_data.append(pedidos_dict)
    return jsonify(pedidos_data)

@app.route('/api/ConsultaPedidoViaTag', methods=['GET'])
@token_required
def get_ConsultaPedidoViaTag():
    codBarras = request.args.get('codBarras')
    TagReposicao = Relatorios.InformacaoPedidoViaTag(codBarras)

    # Obtém os nomes das colunas
    column_names = TagReposicao.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    pedidos_data = []
    for index, row in TagReposicao.iterrows():
        pedidos_dict = {}
        for column_name in column_names:
            pedidos_dict[column_name] = row[column_name]
        pedidos_data.append(pedidos_dict)
    return jsonify(pedidos_data)


@app.route('/api/FilaReposicaoOP', methods=['GET'])
@token_required
def get_FilaReposicaoOP():
    empresa = request.args.get('empresa','1')
    natureza = request.args.get('natureza','5')
    FilaReposicaoOP = OPfilaReporRailway.FilaPorOP(natureza, empresa)
    # Obtém os nomes das colunas
    column_names = FilaReposicaoOP.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    FilaReposicaoOP_data = []
    for index, row in FilaReposicaoOP.iterrows():
        FilaReposicaoOP_dict = {}
        for column_name in column_names:
            FilaReposicaoOP_dict[column_name] = row[column_name]
        FilaReposicaoOP_data.append(FilaReposicaoOP_dict)
    return jsonify(FilaReposicaoOP_data)
@app.route('/api/ObterEmpresasNatureza', methods=['GET'])
@token_required
def ObterEmpresasNatureza():

    FilaReposicaoOP = OPfilaReporRailway.ObterNaturezas()
    # Obtém os nomes das colunas
    column_names = FilaReposicaoOP.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    FilaReposicaoOP_data = []
    for index, row in FilaReposicaoOP.iterrows():
        FilaReposicaoOP_dict = {}
        for column_name in column_names:
            FilaReposicaoOP_dict[column_name] = row[column_name]
        FilaReposicaoOP_data.append(FilaReposicaoOP_dict)
    return jsonify(FilaReposicaoOP_data)

@app.route('/api/AtribuirOPRepositor', methods=['POST'])
@token_required
def get_AtribuirOPRepositor():
    # Obtenha os dados do corpo da requisição
    data = request.get_json()
    OP = data['numeroOP']
    Usuario = data['codigo']
    Reatribuir = data.get('reatribuir', False)  # Valor padrão: False, se 'estornar' não estiver presente no corpo

    #Verifica Se existe atribuicao
    existe = OPfilaReporRailway.ConsultaSeExisteAtribuicao(OP)
    if existe == 0:
        if Reatribuir is True:
            OPfilaReporRailway.AtribuiRepositorOP(Usuario, OP)
            return jsonify({'message': f'OP {OP} reatribuida para o Usuario {Usuario}'})
        else:
            # Retorna uma resposta de existencia
            return jsonify({'message': f'OP já foi Atribuida'})
    else:

        OPfilaReporRailway.AtribuiRepositorOP(Usuario,OP)
        # Retorna uma resposta de sucesso
        return jsonify({'message': True})
@app.route('/api/DetalhaOP', methods=['GET'])
@token_required
def get_DetalhaOP():
    empresa = request.args.get('empresa','1')
    natureza = request.args.get('natureza','5')
    # Obtém o código do usuário e a senha dos parâmetros da URL
    NumeroOP = request.args.get('numeroOP')
    op = OPfilaReporRailway.detalhaOP(NumeroOP,empresa, natureza)
    # Obtém os nomes das colunas
    column_names = op.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    OP_data = []
    for index, row in op.iterrows():
        op_dict = {}
        for column_name in column_names:
            op_dict[column_name] = row[column_name]
        OP_data.append(op_dict)
    return jsonify(OP_data)

@app.route('/api/DetalhaOPxSKU', methods=['GET'])
@token_required
def get_DetalhaOPxSKU():
    # Obtém o código do usuário e a senha dos parâmetros da URL
    NumeroOP = request.args.get('numeroOP')
    empresa = request.args.get('empresa','1')
    natureza = request.args.get('natureza','5')
    op = OPfilaReporRailway.detalhaOPxSKU(NumeroOP, empresa, natureza)
    # Obtém os nomes das colunas
    column_names = op.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    OP_data = []
    for index, row in op.iterrows():
        op_dict = {}
        for column_name in column_names:
            op_dict[column_name] = row[column_name]
        OP_data.append(op_dict)
    return jsonify(OP_data)

@app.route('/api/DetalhaSKU', methods=['GET'])
@token_required
def get_DetalhaSKU():
    # Obtém o código do usuário e a senha dos parâmetros da URL
    codreduzido = request.args.get('codreduzido')
    op = OPfilaReporRailway.detalhaSku(codreduzido)
    # Obtém os nomes das colunas
    column_names = op.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    OP_data = []
    for index, row in op.iterrows():
        op_dict = {}
        for column_name in column_names:
            op_dict[column_name] = row[column_name]
        OP_data.append(op_dict)
    return jsonify(OP_data)

@app.route('/api/Enderecos', methods=['GET'])
@token_required
def get_enderecos():
    enderecos= ReposicaoRailway.ObeterEnderecos()
    # Obtém os nomes das colunas
    column_names = enderecos.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    enderecos_data = []
    for index, row in enderecos.iterrows():
        enderecos_dict = {}
        for column_name in column_names:
            enderecos_dict[column_name] = row[column_name]
        enderecos_data.append(enderecos_dict)
    return jsonify(enderecos_data)
@app.route('/api/NovoEndereco', methods=['PUT'])
@token_required
def criar_enderco():
    # Obtenha os dados do corpo da requisição
    novo_endereco = request.get_json()
    # Extraia os valores dos campos do novo usuário

    rua = novo_endereco.get('rua')
    modulo = novo_endereco.get('modulo')
    posicao = novo_endereco.get('posicao')

    codendereco = ReposicaoRailway.CadEndereco(rua, modulo, posicao)

    # inserir o novo usuário no banco de dados
    return jsonify({'message': f'Novo endereco:{codendereco} criado com sucesso'}), 201
@app.route('/api/DetalhaEndereco', methods=['GET'])
@token_required
def get_DetalhaEndereco():
    # Obtém o código do endereco e a senha dos parâmetros da URL
    Endereco = request.args.get('Endereco')

    Endereco_det = ReposicaoRailway.SituacaoEndereco(Endereco)
    Endereco_det = pd.DataFrame(Endereco_det)
    # Obtém os nomes das colunas
    column_names = Endereco_det.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    end_data = []
    for index, row in Endereco_det.iterrows():
        end_dict = {}
        for column_name in column_names:
            end_dict[column_name] = row[column_name]
        end_data.append(end_dict)
    return jsonify(end_data)
@app.route('/api/DetalhaTag', methods=['GET'])
@token_required
def get_DetalhaTag():
    # Obtém o código do usuário e a senha dos parâmetros da URL
    codbarra = request.args.get('codbarra')
    codbarra, codbarra1 = PediosReporRailway.EndereçoTag(codbarra)
    # Obtém os nomes das colunas
    column_names = codbarra1.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    OP_data = []
    for index, row in codbarra1.iterrows():
        op_dict = {}
        for column_name in column_names:
            op_dict[column_name] = row[column_name]
        OP_data.append(op_dict)
    return jsonify(OP_data)


@app.route('/api/ApontamentoReposicao', methods=['POST'])
@token_required
def get_ApontaReposicao():
    try:
        # Obtenha os dados do corpo da requisição
        data = request.get_json()
        codUsuario = data['codUsuario']
        codbarra = data['codbarra']
        endereco = data['endereco']
        dataHora = data['dataHora']
        estornar = data.get('estornar', False)  # Valor padrão: False, se 'estornar' não estiver presente no corpo

        # Verifica se existe atribuição
        Apontamento = ReposicaoRailway.RetornoLocalCodBarras(codUsuario,codbarra,endereco,dataHora)

        if Apontamento == 'Reposto':
            if estornar:
                ReposicaoRailway.EstornoApontamento(codbarra)
                return jsonify({'message': f'codigoBarras {codbarra} estornado!'})

            ender, ender2 = PediosReporRailway.EndereçoTag(codbarra)
            return jsonify({'message': f'codigoBarras {codbarra} ja reposto no endereço {ender}'})

        if Apontamento is False:
            return jsonify({'message': False, 'Status': f'codigoBarras {codbarra} nao existe no Estoque'})

        return jsonify({'message': True, 'status': f'Salvo com Sucesso'})

    except KeyError as e:
        return jsonify({'message': 'Erro nos dados enviados.', 'error': str(e)}), 400

    except Exception as e:
        return jsonify({'message': 'Ocorreu um erro interno.', 'error': str(e)}), 500


#ETAPA 2:  Api para acesso do Quadro de Estamparia - Projeto WMS das Telas de  Silk:

@app.route('/api/Silk/PesquisaEndereco', methods=['GET'])
@token_required
def get_PesquisaEndereco():
        Coluna = request.args.get('Coluna')
        Operador = request.args.get('Operador')
        Nome = request.args.get('Nome')

        resultados = Silk_PesquisaNew.PesquisaEnderecos(Coluna, Operador, Nome)

        # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
        filaeposicao_data = []
        for row in resultados:
            filaeposicao_dict = {}
            for i, value in enumerate(row):
                filaeposicao_dict[
                    f'col{i + 1}'] = value  # Substitua 'col{i+1}' pelo nome da coluna correspondente, se disponível
            filaeposicao_data.append(filaeposicao_dict)

        return jsonify(filaeposicao_data)
    
@app.route('/api/Silk/deleteTelas', methods=['DELETE'])
@token_required
def delete_endpoint():
    endereco = request.args.get('endereco')
    produto = request.args.get('produto')

    # Chama a função Funcao_Deletar para realizar a exclusão
    resultado = Silk_PesquisaNew.Funcao_Deletar(endereco, produto)

    if resultado == True:
        return f'endereco: {endereco}, produto {produto}  EXCLUIDOS NO CADASTRO DE SILK', 200
    else:
        return 'Falha ao deletar', 500

@app.route('/api/Silk/IserirTelas', methods=['PUT'])
@token_required
def insert_endpoint():
    produto = request.args.get('produto')
    endereco = request.args.get('endereco')

    # Chama a função Funcao_Inserir para realizar a inserção
    resultado = Silk_PesquisaNew.Funcao_Inserir(produto, endereco)

    if resultado == True:
        return f'produto{produto} endereço{endereco}, Inserção realizada com sucesso', 200
    else:
        return 'Falha ao inserir', 500
    
# Api para o processo de inventario
@app.route('/api/RegistrarInventario', methods=['POST'])
@token_required
def get_ProtocolarInventario():
    # Obtém os dados do corpo da requisição (JSON)
    datas = request.get_json()
    codUsuario = datas['codUsuario']
    data = datas['data']
    endereco = datas['endereço']

    Endereco_det = InventarioPrateleira.SituacaoEndereco(endereco, codUsuario, data)
    Endereco_det = pd.DataFrame(Endereco_det)

    # Obtém os nomes das colunas
    column_names = Endereco_det.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    end_data = []
    for index, row in Endereco_det.iterrows():
        end_dict = {}
        for column_name in column_names:
            end_dict[column_name] = row[column_name]
        end_data.append(end_dict)
    return jsonify(end_data)
@app.route('/api/ApontarTagInventario', methods=['POST'])
@token_required
def get_ApontarTagInventario():
    # Obtém os dados do corpo da requisição (JSON)
    datas = request.get_json()
    codbarras = datas['codbarras']
    codusuario = datas['codUsuario']
    endereco = datas['endereço']
    Prosseguir = datas.get('Prosseguir', False)  # Valor padrão: False, se 'estornar' não estiver presente no corpo

    Endereco_det = InventarioPrateleira.ApontarTagInventario(codbarras, endereco, codusuario, Prosseguir)

    # Obtém os nomes das colunas
    column_names = Endereco_det.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    end_data = []
    for index, row in Endereco_det.iterrows():
        end_dict = {}
        for column_name in column_names:
            end_dict[column_name] = row[column_name]
        end_data.append(end_dict)
    return jsonify(end_data)

@app.route('/api/FinalizarInventario', methods=['POST'])
@token_required
def get_FinalizarInventario():
    # Obtém os dados do corpo da requisição (JSON)
    datas = request.get_json()
    endereco = datas['endereço']

    Endereco_det = InventarioPrateleira.SalvarInventario(endereco)
    Endereco_det = pd.DataFrame(Endereco_det)
    # Obtém os nomes das colunas
    column_names = Endereco_det.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    end_data = []
    for index, row in Endereco_det.iterrows():
        end_dict = {}
        for column_name in column_names:
            end_dict[column_name] = row[column_name]
        end_data.append(end_dict)
    return jsonify(end_data)

# Aqui comeca as API's referente aos pedidos
@app.route('/api/FilaPedidos', methods=['GET'])
@token_required
def get_FilaPedidos():
    Incremento.testeAtualizacao(50)
    Pedidos= PediosReporRailway.FilaPedidos()
    # Obtém os nomes das colunas
    column_names = Pedidos.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    pedidos_data = []
    for index, row in Pedidos.iterrows():
        pedidos_dict = {}
        for column_name in column_names:
            pedidos_dict[column_name] = row[column_name]
        pedidos_data.append(pedidos_dict)
    return jsonify(pedidos_data)
@app.route('/api/FilaPedidosUsuario', methods=['GET'])
@token_required
def get_FilaPedidosUsuario():
    codUsuario = request.args.get('codUsuario')
    Pedidos= PediosReporRailway.FilaAtribuidaUsuario(codUsuario)
    # Obtém os nomes das colunas
    column_names = Pedidos.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    pedidos_data = []
    for index, row in Pedidos.iterrows():
        pedidos_dict = {}
        for column_name in column_names:
            pedidos_dict[column_name] = row[column_name]
        pedidos_data.append(pedidos_dict)
    return jsonify(pedidos_data)

@app.route('/api/DetalharPedido', methods=['GET'])
@token_required
def get_DetalharPedido():
    # Obtém os dados do corpo da requisição (JSON)
    codPedido = request.args.get('codPedido')

    Endereco_det = DetalhaPedido.DetalhaPedido(codPedido)
    Endereco_det = pd.DataFrame(Endereco_det)

    # Obtém os nomes das colunas
    column_names = Endereco_det.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    end_data = []
    for index, row in Endereco_det.iterrows():
        end_dict = {}
        for column_name in column_names:
            end_dict[column_name] = row[column_name]
        end_data.append(end_dict)
    return jsonify(end_data)


@app.route('/api/ApontamentoTagPedido', methods=['POST'])
@token_required
def get_ApontamentoTagPedido():
    # Obtém os dados do corpo da requisição (JSON)
    datas = request.get_json()
    codusuario = datas['codUsuario']
    codpedido = datas['codpedido']
    enderecoApi = datas['endereço']
    codbarras = datas['codbarras']
    dataSeparacao = datas['dataHoraBipágem']
    Estornar = datas.get('estornar', False)  # Valor padrão: False, se 'estornar' não estiver presente no corpo

    Endereco_det = PediosReporRailway.ApontamentoTagPedido(str(codusuario), codpedido, codbarras, dataSeparacao,enderecoApi,
                                                          Estornar)

    # Obtém os nomes das colunasok
    column_names = Endereco_det.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    end_data = []
    for index, row in Endereco_det.iterrows():
        end_dict = {}
        for column_name in column_names:
            end_dict[column_name] = row[column_name]
        end_data.append(end_dict)
    return jsonify(end_data)


@app.route('/api/ApontarTagReduzido', methods=['POST'])
@token_required
def get_ApontarTagReduzido():
    # Obtém os dados do corpo da requisição (JSON)
    datas = request.get_json()

    codusuario = datas['codUsuario']
    dataHora = datas['dataHora']
    endereco = datas['endereço']
    codbarra = datas['codbarras']
    Prosseguir = datas.get('Prosseguir', False)  # Valor padrão: False, se 'estornar' não estiver presente no corpo

    Endereco_det = ReposicaoSkuRailway.ApontarTagReduzido(codbarra,endereco,codusuario,'dataHora',Prosseguir)

    # Obtém os nomes das colunas
    column_names = Endereco_det.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    end_data = []
    for index, row in Endereco_det.iterrows():
        end_dict = {}
        for column_name in column_names:
            end_dict[column_name] = row[column_name]
        end_data.append(end_dict)
    return jsonify(end_data)

@app.route('/api/RelatorioEndereços', methods=['GET'])

def get_RelatorioEndereços():
    # Obtém os dados do corpo da requisição (JSON)


    Endereco_det = Relatorios.relatorioEndereços()

    # Obtém os nomes das colunas
    column_names = Endereco_det.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    end_data = []
    for index, row in Endereco_det.iterrows():
        end_dict = {}
        for column_name in column_names:
            end_dict[column_name] = row[column_name]
        end_data.append(end_dict)
    return jsonify(end_data)

@app.route('/api/RelatorioFila', methods=['GET'])
def get_RelatorioFila():
    # Obtém os dados do corpo da requisição (JSON)
    Endereco_det = Relatorios.relatorioFila()

    # Obtém os nomes das colunas
    column_names = Endereco_det.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    end_data = []
    for index, row in Endereco_det.iterrows():
        end_dict = {}
        for column_name in column_names:
            end_dict[column_name] = row[column_name]
        end_data.append(end_dict)
    return jsonify(end_data)
@app.route('/api/RelatorioTotalFila', methods=['GET'])
@token_required
def get_RelatorioTotalFila():
        # Obtém os dados do corpo da requisição (JSON)


        Endereco_det = Relatorios.relatorioTotalFila()
        Endereco_det = pd.DataFrame(Endereco_det)
        # Obtém os nomes das colunas
        column_names = Endereco_det.columns
        # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
        end_data = []
        for index, row in Endereco_det.iterrows():
            end_dict = {}
            for column_name in column_names:
                end_dict[column_name] = row[column_name]
            end_data.append(end_dict)
        return jsonify(end_data)

@app.route('/api/ListagemErros', methods=['GET'])

def get_ListagemErros():
    # Obtém os dados do corpo da requisição (JSON)


    Endereco_det = TratamentoErrosRailway.ListaErros()

    # Obtém os nomes das colunas
    column_names = Endereco_det.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    end_data = []
    for index, row in Endereco_det.iterrows():
        end_dict = {}
        for column_name in column_names:
            end_dict[column_name] = row[column_name]
        end_data.append(end_dict)
    return jsonify(end_data)
@app.route('/api/AtribuirPedidos', methods=['POST'])
@token_required
def get_AtribuirPedidos():
    try:
        # Obtém os dados do corpo da requisição (JSON)
        datas = request.get_json()
        codUsuario = datas['codUsuario']
        data = datas['data']
        pedidos = datas['pedidos']

        Endereco_det = DistribuicaoPedidosRailway.AtribuirPedido(codUsuario,pedidos,data)
        Endereco_det = pd.DataFrame(Endereco_det)

        # Obtém os nomes das colunas
        column_names = Endereco_det.columns
        # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
        end_data = []
        for index, row in Endereco_det.iterrows():
            end_dict = {}
            for column_name in column_names:
                end_dict[column_name] = row[column_name]
            end_data.append(end_dict)
        return jsonify(end_data)
    except KeyError as e:
        return jsonify({'message': 'Erro nos dados enviados.', 'error': str(e)}), 400

    except Exception as e:
        return jsonify({'message': 'Ocorreu um erro interno.', 'error': str(e)}), 500
@app.route('/api/AtualizaEnderecoPedidoss', methods=['POST'])
@token_required
def get_AtualizaEnderecoPedidoss():
    try:
        # Obtém os dados do corpo da requisição (JSON)
        datas = request.get_json()
        iteracoes = datas['iteracoes']


        Endereco_det = Incremento.testeAtualizacao(iteracoes)
        Endereco_det = pd.DataFrame(Endereco_det)

        # Obtém os nomes das colunas
        column_names = Endereco_det.columns
        # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
        end_data = []
        for index, row in Endereco_det.iterrows():
            end_dict = {}
            for column_name in column_names:
                end_dict[column_name] = row[column_name]
            end_data.append(end_dict)
        return jsonify(end_data)
    except KeyError as e:
        return jsonify({'message': 'Erro nos dados enviados.', 'error': str(e)}), 400

    except Exception as e:
        return jsonify({'message': 'Ocorreu um erro interno.', 'error': str(e)}), 500
@app.route('/api/NecessidadeReposicao', methods=['GET'])
@token_required
def get_RelatorioNecessidadeReposicao():
    # Obtém os dados do corpo da requisição (JSON)


    Endereco_det = Relatorios.RelatorioNecessidadeReposicao()
    Endereco_det = pd.DataFrame(Endereco_det)
    # Obtém os nomes das colunas
    column_names = Endereco_det.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    end_data = []
    for index, row in Endereco_det.iterrows():
        end_dict = {}
        for column_name in column_names:
            end_dict[column_name] = row[column_name]
        end_data.append(end_dict)
    return jsonify(end_data)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=port)

