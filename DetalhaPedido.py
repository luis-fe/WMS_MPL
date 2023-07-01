import pandas as pd
import ConexaoPostgreRailway

def DetalhaPedido(codPedido):
    # 1- Filtrando o Pedido na tabela de pedidosSku
    conn = ConexaoPostgreRailway.conexao()
    skus = pd.read_sql('select codigopedido, desc_tiponota  , codcliente ||' + "'-'" + '|| desc_cliente as cliente  '
                                                                                       ',codrepresentante  ||' + "'-'" + '|| desc_representante  as repres, agrupamentopedido '
                                                                                                                         'from "Reposicao".filaseparacaopedidos f  where codigopedido= ' + "'" + codPedido + "'"
                       , conn)
    grupo = pd.read_sql('select agrupamentopedido '
                        'from "Reposicao".filaseparacaopedidos f  where codigopedido= ' + "'" + codPedido + "'"
                        , conn)
    DetalhaSku = pd.read_sql(
        'select  produto as reduzido, qtdesugerida , status as concluido_X_total, endereco as endereco, necessidade as a_concluir , '
        'qtdesugerida as total, (qtdesugerida - necessidade) as qtdrealizado'
        ' from "Reposicao".pedidossku p  where codpedido= ' + "'" + codPedido + "'"
                                                                                " order by endereco asc", conn)

    # Validando as descricoes + cor + tamanho dos produtos para nao ser null

    validador1 = pd.read_sql('select distinct * from '
                             '(select p.codpedido , p.produto , t.codreduzido  from "Reposicao".pedidossku p '
                             'left join "Reposicao".tagsreposicao t on t.codreduzido = p.produto) as var '
                             'where var.codreduzido is null and '
                             "var.codpedido = '"+codPedido+"'",conn)
    if validador1.empty:
        descricaoSku = pd.read_sql(
            'select  f.engenharia as referencia, f."codreduzido" as reduzido, f."descricao" , f."cor" , f.tamanho  from "Reposicao".tagsreposicao f  '
            'where f."codreduzido" in '
            '(select  produto as reduzido '
            ' from "Reposicao".pedidossku p  where codpedido = ' + "'" + codPedido + "') "
                                                                                     'group by f."codreduzido", f.descricao , f."cor" , f.tamanho , f.engenharia',
            conn)
        print(f'Pedido {codPedido} Detalhado Pela Tabela de  Reposicao')
    else:
        validadorFilas = pd.read_sql('select p.codpedido , p.produto , t.codreduzido  from "Reposicao".pedidossku p '
                                 'left join "Reposicao".filareposicaoportag t on t.codreduzido = p.produto '
                                 'where t.codreduzido is null and '
                                 "p.codpedido = '" + codPedido + "'", conn)

        if validadorFilas.empty:
            descricaoSku = pd.read_sql(
                'select  f.engenharia as referencia, f."codreduzido" as reduzido, f."descricao" , f."cor" , f.tamanho  from "Reposicao".filareposicaoportag f  '
                'where f."codreduzido" in '
                '(select  produto as reduzido '
                ' from "Reposicao".pedidossku p  where codpedido = ' + "'" + codPedido + "') "
                                                                                         'group by f."codreduzido", f.descricao , f."cor" , f.tamanho , f.engenharia',
                conn)
            print(f'Pedido {codPedido} Detalhado Pela Tabela da Fila')

        else:
            validador2 = pd.read_sql('SELECT p.codpedido, p.produto '
                                 'FROM "Reposicao".pedidossku p '
                                 'LEFT JOIN ( '
                                 '     SELECT f.codreduzido AS codreduzido'
                                 '     FROM "Reposicao".filareposicaoportag f '
                                 ' union '
                                 '     SELECT t.codreduzido AS codreduzido'
                                 '     FROM "Reposicao".tagsreposicao t '
                                 ' ) AS procurar ON procurar.codreduzido = p.produto'
                                 " where p.codpedido = '"+codPedido+"' "
                                                                   "AND procurar.codreduzido IS NULL ", conn)

            if validador2.empty:
                descricaoSku = pd.read_sql(
                'select  f.engenharia as referencia, f."codreduzido" as reduzido, f."descricao" , f."cor" , f.tamanho  from "Reposicao".tagsreposicao f '
                ' where f."codreduzido" in '
                '(select  produto as reduzido '
                'from "Reposicao".pedidossku p  where codpedido = ' + "'" + codPedido + "') "
                'group by f."codreduzido", f.descricao , f."cor" , f.tamanho , f.engenharia'                        
                ' union '
                'select t.engenharia as referencia, t."codreduzido", t."descricao" , t.cor , t.tamanho  from "Reposicao".filareposicaoportag t '
                ' where t.descricao is not null and'
                ' t."codreduzido" in '
                '(select  produto as reduzido '
                'from "Reposicao".pedidossku p  where codpedido = ' + "'" + codPedido + "') "
                'group by t."codreduzido", t.descricao , t."cor" , t.tamanho , t.engenharia', conn)

                print(f'Pedido {codPedido} Detalhado Pela Tabela de  Reposicao + Fila')

            else:
                descricaoSku = pd.read_sql(
                    'select  f.engenharia as referencia, f."codreduzido" as reduzido, f."descricao" , f."cor" , f.tamanho  from "Reposicao".tagsreposicao f '
                    ' where f."codreduzido" in '
                    '(select  produto as reduzido '
                    'from "Reposicao".pedidossku p  where codpedido = ' + "'" + codPedido + "') "
                    'group by f."codreduzido", f.descricao , f."cor" , f.tamanho , f.engenharia'
                    ' union '
                    'select t.engenharia as referencia, t."codreduzido", t."descricao" , t.cor , t.tamanho  from "Reposicao".filareposicaoportag t '
                    ' where t.descricao is not null '
                    ' and t."codreduzido" in '
                    '(select  produto as reduzido '
                    'from "Reposicao".pedidossku p  where codpedido = ' + "'" + codPedido + "') "
                    'group by t."codreduzido", t.descricao , t."cor" , t.tamanho , t.engenharia'
                ' union'
                ' select  f.engenharia as referencia, f."codreduzido" as reduzido, f."descricao" , f."cor" , f.tamanho  from "Reposicao".tags_separacao f '
                ' where f."codreduzido" in '
                '(select  produto as reduzido '
                'from "Reposicao".pedidossku p  where codpedido = ' + "'" + codPedido + "') "
                'group by f."codreduzido", f.descricao , f."cor" , f.tamanho , f.engenharia' , conn)
                print(f'Pedido {codPedido} Detalhado Pela Tabela de  Reposicao + Fila + Separacao')

    # continuacao do codigo
    descricaoSku.drop_duplicates(subset=('reduzido', 'referencia'), inplace=True)

    DetalhaSku = pd.merge(DetalhaSku, descricaoSku, on='reduzido', how='left')
    DetalhaSku.drop_duplicates(subset=('reduzido', 'referencia'), inplace=True)

    data = {
        '1 - codpedido': f'{skus["codigopedido"][0]} ',
        '2 - Tiponota': f'{skus["desc_tiponota"][0]} ',
        '3 - Cliente': f'{skus["cliente"][0]} ',
        '4- Repres.': f'{skus["repres"][0]} ',
        # '4.1- Grupo.': f'{skus["grupo"][0]} ',
        '5- Detalhamento dos Sku:': DetalhaSku.to_dict(orient='records')
    }
    return [data]

def AtualizadoEnderecoPedido(codpedido):
    conn = ConexaoPostgreRailway.conexao()
    Pedido = pd.read_sql('Select * From "Reposicao".pedidossku p '
                         'where p.codpedido = '+"'"+codpedido+"'"+" and p.necessidade > 0",conn)
    testeAtualizacao(Pedido)
    return print(f'pedido {codpedido} atualizado')


def testeAtualizacao(dataframe):
    dataframe = dataframe
    tamanho = dataframe['codpedido'].size
    conn = ConexaoPostgreRailway.conexao()
    if dataframe.empty:
        print('sem incrmento' )
        return pd.DataFrame(
                {'Mensagem': [f'{tamanho} atualizacoes para realizar! ']})
    else:
        for i in range(tamanho):

            query = '''
                UPDATE "Reposicao".pedidossku p 
                SET "endereco" = %s
                WHERE p.codpedido = %s AND p.produto = %s
            '''

            # Execute a consulta usando a conexão e o cursor apropriados
            cursor = conn.cursor()
            cursor.execute(query, (dataframe['endereco'][i], dataframe['codpedido'][i], dataframe['produto'][i]))
            conn.commit()
