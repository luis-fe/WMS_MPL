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
        return False, pd.DataFrame({'Mensagem': [f'tag nao encontrada']})


def FilaPedidos():
    conn = ConexaoPostgreRailway.conexao()
    pedido = pd.read_sql(' select f.codigopedido , f.vlrsugestao, f.codcliente , f.desc_cliente, f.cod_usuario, f.cidade, f.estado, '
                         'datageracao, f.codrepresentante , f.desc_representante, f.desc_tiponota, condicaopgto, agrupamentopedido  ' 
                        '  from "Reposicao".filaseparacaopedidos f ',conn)
    pedidosku = pd.read_sql('select codpedido, sum(qtdesugerida) as qtdesugerida  from "Reposicao".pedidossku p  '
                            'group by codpedido ',conn)
    pedidosku.rename(columns={'codpedido':'01-CodPedido', 'qtdesugerida':'15-qtdesugerida'},inplace=True)

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

    pedido = pd.merge(pedido,pedidosku,on='01-CodPedido',how='left')


    return pedido

def FilaAtribuidaUsuario(codUsuario):
    x = FilaPedidos()
    codUsuario = str(codUsuario)
    x = x[x['10-codUsuarioAtribuido'] == codUsuario]
    return x


def DetalhaPedido(codPedido):
    conn = ConexaoPostgreRailway.conexao()
    skus = pd.read_sql('select codigopedido, desc_tiponota  , codcliente ||'+"'-'"+'|| desc_cliente as cliente  '
                                ',codrepresentante  ||'+"'-'"+'|| desc_representante  as repres '
                                'from "Reposicao".filaseparacaopedidos f  where codigopedido= '+"'"+codPedido+"'"
                                ,conn)
    DetalhaSku = pd.read_sql('select  produto as reduzido, qtdesugerida , status as concluido_X_total, endereco as endereco'
                            ' from "Reposicao".pedidossku p  where codpedido= '+"'"+codPedido+"'",conn)
    descricaoSku = pd.read_sql( 'select f."codReduzido" as reduzido, f."descricao" , f."Cor" , f.tamanho  from "Reposicao".filareposicaoportag f '
                                'group by f."codReduzido", f.descricao , f."Cor" , f.tamanho '
                                ' union '
                                'select t."CodReduzido", t."Descricao" , t.cor , t.tamanho  from "Reposicao".tagsreposicao t '
                                'group by  t."CodReduzido", t."Descricao" , t.cor , t.tamanho',conn)
    descricaoSku.drop_duplicates(subset='reduzido', inplace=True)
    DetalhaSku = pd.merge(DetalhaSku,descricaoSku,on='reduzido',how='left')

    data = {
        '1 - codpedido': f'{skus["codigopedido"][0]} ',
        '2 - Tiponota': f'{skus["desc_tiponota"][0]} ',
        '3 - Cliente': f'{skus["cliente"][0]} ',
        '4- Repres.': f'{skus["repres"][0]} ',
        '5- Detalhamento dos Sku:': DetalhaSku.to_dict(orient='records')
    }
    return [data]

def ApontamentoTagPedido(codusuario, codpedido, codbarra, endereco):
    validacao = VerificacoesApontamento(codbarra)
    if validacao == 1:
        conn = ConexaoPostgreRailway.conexao()
        insert = 'INSERT INTO "Reposicao".tags_separacao ("Usuario", "codbarrastag", "CodReduzido", "Endereco", ' \
                 '"Engenharia", "DataReposicao", "Descricao", "Epc", "StatusEndereco", ' \
                 '"numeroop", "cor", "tamanho", "totalop", "codpedido") ' \
                 'SELECT %s, "codbarrastag", "CodReduzido", "Endereco", "Engenharia", ' \
                 '"DataReposicao", "Descricao", "Epc", %s, "numeroop", "cor", "tamanho", "totalop", ' \
                 "%s" \
                 'FROM "Reposicao".tagsreposicao t ' \
                 'WHERE "codbarrastag" = %s;'
        cursor = conn.cursor()
        cursor.execute(insert, (codusuario,'tagSeparado',codpedido, codbarra))
        conn.commit()
        cursor.close()
        delete = 'Delete from "Reposicao"."tagsreposicao"  ' \
                 'where "codbarrastag" = %s;'
        cursor = conn.cursor()
        cursor.execute(delete
                       , (
                           codbarra,))
        conn.commit()
        cursor.close()
        conn.close()
        return pd.DataFrame({'Mensagem': [f'tag {codbarra} apontada!']})
    else:
        return pd.DataFrame({'Mensagem': [f'tag nao encontrada no estoque do endereço']})




def VerificacoesApontamento(codbarra):
    conn = ConexaoPostgreRailway.conexao()
    pesquisa = pd.read_sql(
        ' select codbarrastag   from "Reposicao".tagsreposicao f   '
        'where codbarrastag = ' + "'" + codbarra + "'", conn)
    conn.close()
    if not pesquisa.empty:
        return 1
    else:
        return 0

