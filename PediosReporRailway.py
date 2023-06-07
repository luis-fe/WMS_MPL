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
        " select '-' as Endereço  from "+'"Reposicao".filareposicaoportag f   '
        'where codbarrastag = ' + "'" + codbarra + "'", conn)

    pesquisa2['Situacao'] = 'na fila'
    pesquisa3 = pd.read_sql(
        ' select f.Endereco  from "Reposicao".tagsreposicao_inventario f   '
        'where codbarrastag = ' + "'" + codbarra + "'", conn)

    pesquisa3['Situacao'] = f'em inventario no endereço: {pesquisa3["Endereco"][0]}'
    conn.close()

    if not pesquisa2.empty:
        return False, pesquisa2

    if not pesquisa3.empty:
        return False, pesquisa3
    else:

        return pesquisa['Endereco'][0], pesquisa

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

