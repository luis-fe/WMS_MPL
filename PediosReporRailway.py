import ConexaoPostgreRailway
import pandas as pd

import numpy


def EndereçoTag(codbarra):
    conn = ConexaoPostgreRailway.conexao()
    pesquisa = pd.read_sql(
        ' select t."Endereco"  from "Reposicao".tagsreposicao t  '
        'where codbarrastag = ' + "'" + codbarra + "'", conn)

    pesquisa['Situacao'] = 'Reposto'
    pesquisa2 = pd.read_sql(
        " select '-' as Endereco  from " + '"Reposicao".filareposicaoportag f   '
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
    pedido = pd.read_sql(
        ' select f.codigopedido , f.vlrsugestao, f.codcliente , f.desc_cliente, f.cod_usuario, f.cidade, f.estado, '
        'datageracao, f.codrepresentante , f.desc_representante, f.desc_tiponota, condicaopgto, agrupamentopedido  '
        '  from "Reposicao".filaseparacaopedidos f ', conn)
    pedidosku = pd.read_sql('select codpedido, sum(qtdesugerida) as qtdesugerida, sum(necessidade) as necessidade   from "Reposicao".pedidossku p  '
                            'group by codpedido ', conn)
    pedidosku.rename(columns={'codpedido': '01-CodPedido', 'qtdesugerida': '15-qtdesugerida','necessidade': '19-necessidade'}, inplace=True)

    usuarios = pd.read_sql(
        'select codigo as cod_usuario , nome as nomeusuario_atribuido  from "Reposicao".cadusuarios c ', conn)
    usuarios['cod_usuario'] = usuarios['cod_usuario'].astype(str)
    pedido = pd.merge(pedido, usuarios, on='cod_usuario', how='left')

    pedido.rename(
        columns={'codigopedido': '01-CodPedido', 'datageracao': '02- Data Sugestao', 'desc_tiponota': '03-TipoNota',
                 'codcliente': '04-codcliente',
                 'desc_cliente': '05-desc_cliente', 'cidade': '06-cidade', 'estado': '07-estado',
                 'codrepresentante': '08-codrepresentante', 'desc_representante': '09-Repesentante',
                 'cod_usuario': '10-codUsuarioAtribuido',
                 'nomeusuario_atribuido': '11-NomeUsuarioAtribuido', 'vlrsugestao': '12-vlrsugestao',
                 'condicaopgto': '13-CondPgto', 'agrupamentopedido': '14-AgrupamentosPedido'}, inplace=True)

    pedido['12-vlrsugestao'] = 'R$ ' + pedido['12-vlrsugestao']

    pedido = pd.merge(pedido, pedidosku, on='01-CodPedido', how='left')
    pedido['15-qtdesugerida'] = pedido['15-qtdesugerida'].fillna(0)
    pedidoskuReposto = pd.read_sql('select codpedido, sum(necessidade) as reposto  from "Reposicao".pedidossku p '
                                   "where endereco <> 'Não Reposto' "
                                   'group by codpedido ', conn)
    pedidoskuReposto.rename(columns={'codpedido': '01-CodPedido', 'reposto': '16-Endereco Reposto'}, inplace=True)
    pedido = pd.merge(pedido, pedidoskuReposto, on='01-CodPedido', how='left')
    pedidoskuReposto2 = pd.read_sql('select codpedido, sum(necessidade) as naoreposto  from "Reposicao".pedidossku p '
                                    "where endereco = 'Não Reposto' "
                                    'group by codpedido ', conn)
    pedidoskuReposto2.rename(columns={'codpedido': '01-CodPedido', 'naoreposto': '17-Endereco NaoReposto'},
                             inplace=True)
    pedido = pd.merge(pedido, pedidoskuReposto2, on='01-CodPedido', how='left')
    pedido['16-Endereco Reposto'] = pedido['16-Endereco Reposto'].fillna(0)
    pedido['17-Endereco NaoReposto'] = pedido['17-Endereco NaoReposto'].fillna(0)
    pedido['19-necessidade'] = pedido['19-necessidade'].fillna(0)

    pedido['18-%Reposto'] = pedido['17-Endereco NaoReposto'] + pedido['16-Endereco Reposto']
    pedido['18-%Reposto'] = pedido['16-Endereco Reposto'] / pedido['18-%Reposto']
    pedido['20-Separado%'] = 1 - (pedido['19-necessidade'] / pedido['15-qtdesugerida'])
    pedido['18-%Reposto'] = (pedido['18-%Reposto'] * 100).round(2)
    pedido['18-%Reposto'] = pedido['18-%Reposto'].fillna(0)
    pedido['20-Separado%'] = (pedido['20-Separado%'] * 100).round(2)
    pedido['20-Separado%'] = pedido['20-Separado%'].fillna(0)
    return pedido


def FilaAtribuidaUsuario(codUsuario):
    x = FilaPedidos()
    codUsuario = str(codUsuario)
    x = x[x['10-codUsuarioAtribuido'] == codUsuario]
    return x


def DetalhaPedido(codPedido):
    conn = ConexaoPostgreRailway.conexao()
    skus = pd.read_sql('select codigopedido, desc_tiponota  , codcliente ||' + "'-'" + '|| desc_cliente as cliente  '
                                                                                       ',codrepresentante  ||' + "'-'" + '|| desc_representante  as repres, agrupamentopedido '
                                                                                                                         'from "Reposicao".filaseparacaopedidos f  where codigopedido= ' + "'" + codPedido + "'"
                       , conn)
    grupo = pd.read_sql('select agrupamentopedido '
                        'from "Reposicao".filaseparacaopedidos f  where codigopedido= ' + "'" + codPedido + "'"
                        , conn)
    DetalhaSku = pd.read_sql(
        'select  produto as reduzido, qtdesugerida , status as concluido_X_total, endereco as endereco'
        ' from "Reposicao".pedidossku p  where codpedido= ' + "'" + codPedido + "'"
                                                                                " order by endereco asc", conn)
    descricaoSku = pd.read_sql(
        'select  f.engenharia as referencia, f."codreduzido" as reduzido, f."descricao" , f."cor" , f.tamanho  from "Reposicao".tagsreposicao f '
        'group by f."codreduzido", f.descricao , f."cor" , f.tamanho , f.engenharia'
        ' union '
        'select  f.engenharia as referencia, f."codreduzido" as reduzido, f."descricao" , f."cor" , f.tamanho  from "Reposicao".tags_separacao f '
        'group by f."codreduzido", f.descricao , f."cor" , f.tamanho , f.engenharia'
        ' union '
        'select t.engenharia as referencia, t."codreduzido", t."descricao" , t.cor , t.tamanho  from "Reposicao".filareposicaoportag t '
        ' where t.descricao is not null '
        'group by  t."codreduzido", t."descricao" , t.cor , t.tamanho, t.engenharia', conn)
    descricaoSku.drop_duplicates(subset='reduzido', inplace=True)

    DetalhaSku = pd.merge(DetalhaSku, descricaoSku, on='reduzido', how='left')

    data = {
        '1 - codpedido': f'{skus["codigopedido"][0]} ',
        '2 - Tiponota': f'{skus["desc_tiponota"][0]} ',
        '3 - Cliente': f'{skus["cliente"][0]} ',
        '4- Repres.': f'{skus["repres"][0]} ',
        # '4.1- Grupo.': f'{skus["grupo"][0]} ',
        '5- Detalhamento dos Sku:': DetalhaSku.to_dict(orient='records')
    }
    return [data]


def ApontamentoTagPedido(codusuario, codpedido, codbarra, endereco, datahora):
    validacao, colunaReduzido, colunaNecessidade = VerificacoesApontamento(codbarra, codpedido)
    if validacao == 1:
        if colunaNecessidade <= 0:
            return pd.DataFrame(
                {'Mensagem': [f'o produto {colunaReduzido} já foi totalmente bipado. Deseja estornar ?']})
        else:
            conn = ConexaoPostgreRailway.conexao()
            insert = 'INSERT INTO "Reposicao".tags_separacao ("usuario", "codbarrastag", "codreduzido", "Endereco", ' \
                     '"engenharia", "DataReposicao", "descricao", "epc", "StatusEndereco", ' \
                     '"numeroop", "cor", "tamanho", "totalop", "codpedido","dataseparacao") ' \
                     'SELECT %s, "codbarrastag", "codreduzido", "Endereco", "engenharia", ' \
                     '"DataReposicao", "descricao", "epc", %s, "numeroop", "cor", "tamanho", "totalop", ' \
                     "%s, %s " \
                     'FROM "Reposicao".tagsreposicao t ' \
                     'WHERE "codbarrastag" = %s;'
            cursor = conn.cursor()
            cursor.execute(insert, (codusuario, 'tagSeparado', codpedido, datahora, codbarra))
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
            uptadePedido = 'UPDATE "Reposicao".pedidossku' \
                           ' SET necessidade= %s ' \
                           'where "produto" = %s and codpedido= %s ;'
            colunaNecessidade = colunaNecessidade - 1
            cursor = conn.cursor()
            cursor.execute(uptadePedido
                           , (
                               colunaNecessidade, colunaReduzido, codpedido))
            conn.commit()
            cursor.close()

            # atualizando a necessidade
            conn.close()
            return pd.DataFrame({'Mensagem': [f'tag {codbarra} apontada!'], 'status': [True]})
    if validacao == 2:
        return pd.DataFrame({'Mensagem': [f'pedido {codpedido} nao encontrado']})
    else:
        return pd.DataFrame({'Mensagem': [f'tag nao encontrada no estoque do endereço']})


def VerificacoesApontamento(codbarra, codpedido):
    conn = ConexaoPostgreRailway.conexao()
    pesquisa = pd.read_sql(
        ' select "codbarrastag", "codreduzido" as codreduzido  from "Reposicao".tagsreposicao f   '
        'where codbarrastag = ' + "'" + codbarra + "'", conn)

    if not pesquisa.empty:
        pesquisa2 = pd.read_sql(
            ' select p.codpedido, p.produto , p.necessidade  from "Reposicao".pedidossku p    '
            'where codpedido = ' + "'" + codpedido + "' and produto = " + "'" + pesquisa['codreduzido'][0] + "'", conn)
        conn.close()
        if not pesquisa2.empty:
            return 1, pesquisa['codreduzido'][0], pesquisa2['necessidade'][0]
        else:
            return 2, pesquisa['codreduzido'][0], 2
    else:
        conn.close()
        return 0, 0, 0


def pesquisarSKUxPedido(codpedido, reduzido):
    conn = ConexaoPostgreRailway.conexao()
    pesquisa2 = pd.read_sql(
        ' select p.codpedido, p.produto , p.necessidade  from "Reposicao".pedidossku p    '
        'where codpedido = ' + "'" + codpedido + "' and produto = " + "'" + reduzido + "'", conn)
    conn.close()
    return pesquisa2

# print(pesquisarSKUxPedido('290175','591184'))