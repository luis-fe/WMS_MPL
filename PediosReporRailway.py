import ConexaoPostgreRailway
import pandas as pd
import numpy



def FilaPedidos():
    conn = ConexaoPostgreRailway.conexao()
    pedido = pd.read_sql(' select f.codigopedido , f.vlrsugestao, f.codcliente , f.desc_cliente, f.cod_usuario, '
                         'datageracao, f.codrepresentante , f.desc_representante, f.desc_tiponota ' 
                        '  from "Reposicao".filaseparacaopedidos f ',conn)

    usuarios = pd.read_sql(
        'select codigo as cod_usuario , nome as nomeusuario_atribuido  from "Reposicao".cadusuarios c ', conn)
    usuarios['cod_usuario'] = usuarios['cod_usuario'].astype(str)
    pedido = pd.merge(pedido, usuarios, on='cod_usuario', how='left')
    pedido.rename(columns={'codigopedido': '1-CodPedido','datageracao':'2- datageracao','desc_tiponota':'3-TipoNota','codcliente' :'4-codcliente',
                           'desc_cliente':'5-desc_cliente' }, inplace=True)

    return pedido


