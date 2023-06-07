import ConexaoPostgreRailway
import pandas as pd

import numpy

def EndereçoTag(codbarra):
    conn = ConexaoPostgreRailway.conexao()
    pesquisa = pd.read_sql(
        ' select t."Endereco"  from "Reposicao".tagsreposicao t  '
        'where codbarrastag = '+"'"+codbarra+"'", conn)

    pesquisa['Situacao'] = 'Reposto'
    pesquisa2 = pd.read_sql(
        " select '-' as Endereco  from "+'"Reposicao".filareposicaoportag f   '
        'where codbarrastag = ' + "'" + codbarra + "'", conn)

    pesquisa2['Situacao'] = 'na fila'
    pesquisa3 = pd.read_sql(
        ' select distinct f."Endereco"  from "Reposicao".tagsreposicao_inventario f   '
        'where codbarrastag = ' + "'" + codbarra + "'", conn)

    pesquisa3['Situacao'] = f'em inventario'
    conn.close()

    if not pesquisa2.empty:
        return 'Na fila ', pesquisa2

    if not pesquisa3.empty:
        return 'em inventario ', pesquisa3
    if not pesquisa.empty:

        return pesquisa['Endereco'][0], pesquisa
    else:
        return False, pd.DataFrame({'Mensagem': [False]})


def FilaPedidos():
    conn = ConexaoPostgreRailway.conexao()
    pedido = pd.read_sql(' select f.codigopedido , f.vlrsugestao, f.codcliente , f.desc_cliente, f.cod_usuario, f.cidade, f.estado, '
                         'datageracao, f.codrepresentante , f.desc_representante, f.desc_tiponota, condicaopgto, agrupamentopedido  ' 
                        '  from "Reposicao".filaseparacaopedidos f ',conn)

    usuarios = pd.read_sql(
        'select codigo as cod_usuario , nome as nomeusuario_atribuido  from "Reposicao".cadusuarios c ', conn)
    usuarios['cod_usuario'] = usuarios['cod_usuario'].astype(str)
    pedido = pd.merge(pedido, usuarios, on='cod_usuario', how='left')

    pedido.rename(columns={'codigopedido': '01-CodPedido','datageracao':'02- Data Sugestao','desc_tiponota':'03-TipoNota','codcliente' :'04-codcliente',
                           'desc_cliente':'05-desc_cliente', 'cidade':'06-cidade', 'estado':'07-estado',
                           'codrepresentante':'08-codrepresentante', 'desc_representante':'09-Repesentante','cod_usuario' :'10-codUsuarioAtribuido',
                           'nomeusuario_atribuido':'11-NomeUsuarioAtribuido','vlrsugestao':'12-vlrsugestao',
                           'condicaopgto':'13-CondPgto' , 'agrupamentopedido':'14-AgrupamentosPedido' }, inplace=True)

    pedido['12-vlrsugestao'] = 'R$ '+pedido['12-vlrsugestao']

    return pedido

def FilaAtribuidaUsuario(codUsuario):
    x = FilaPedidos()
    codUsuario = str(codUsuario)
    x = x[x['10-codUsuarioAtribuido'] == codUsuario]
    return x

def NecessidadesPedidos():
    # Trazer o dataframe das sugestoes em abertos a nivel de sku
    conn = ConexaoPostgreRailway.conexao()
    pedidos = pd.read_sql()

    # criar uma coluna para a necessidade do sku , segundo o criterio de ordenamento

    # passo 3: criar uma coluna que busca no primeiro endereço se a quantidade atende


    # Replicando a regra para os proximos endereços.

    return 'calcaular a necessidade dos pedidos'

def ApontamentoTagSeparacao():

    return 'API de apontar as tags separadas'