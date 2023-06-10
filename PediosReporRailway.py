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

def NecessidadesPedidos():
    # Trazer o dataframe das sugestoes em abertos a nivel de sku
    conn = ConexaoPostgreRailway.conexao()
    pedidos = pd.read_sql('select codigopedido as codpedido, codtiponota , agrupamentopedido  from "Reposicao".filaseparacaopedidos f ',conn)
    pedidossku = pd.read_sql(
        'select codpedido, necessidade, produto as codreduzido  from "Reposicao".pedidossku p  '
        'where necessidade > 0',conn)
    pedidos = pd.merge(pedidos,pedidossku,on='codpedido')
    # Passo 2 : criterio de prioridade
    # Passo 3: criar uma coluna para a necessidade do sku , segundo o criterio de ordenamento
    pedidos['necessidadeSKU'] =pedidos.groupby('codreduzido').cumcount() + 1

    # passo 4: data frame do estoque por endereco
    estoque = pd.read_sql('select "Endereco" , "CodReduzido" as codreduzido , count("CodReduzido") as saldo  from "Reposicao".tagsreposicao t '
                          'group by "Endereco" , "CodReduzido" ',conn)
    estoque = estoque.sort_values(by='saldo', ascending=False)  # escolher como deseja classificar
    estoque['ordemAparicao'] =estoque.groupby('codreduzido').cumcount() + 1
        #4.1 Obtendo o ordenamento max do estoque
    maxOrdem = max(estoque['ordemAparicao'])

    #Passo 5 - criando colunas de acordo com o numero maximo de estoque encontrado:
    for i in range(maxOrdem):
        col_name = f'endereco -{i+1}'  # Nome da coluna baseado no valor do loop
        col_name2 = f'saldo -{i + 1}'  # Nome da coluna baseado no valor do loop
        enderecoi = estoque[estoque['ordemAparicao']==(i+1)]
        enderecoi = enderecoi[['codreduzido', 'Endereco', 'saldo']]  # Especificar as colunas 'A' e 'C'
        enderecoi[col_name] = enderecoi['Endereco']  # Criar a coluna com o valor do loop
        enderecoi[col_name2] = enderecoi['saldo']  # Criar a coluna com o valor do loop
        enderecoi[col_name2] = enderecoi[col_name2].astype(int)
        pedidos = pd.merge(pedidos, enderecoi, on='codreduzido',how='left')
    pedidos['Necessidade Endereco'] = 0
    pedidos['Endereco official'] = '%'
    for i in range(maxOrdem):
        col_name = f'endereco -{i+1}'  # Nome da coluna baseado no valor do loop
        col_name2 = f'saldo -{i + 1}'  # Nome da coluna baseado no valor do loop
        pedidos[col_name2] = pedidos[col_name2].replace('', numpy.nan).fillna(0)

        pedidos["Necessidade Endereco"] = pedidos.apply(
            lambda row: row['necessidadeSKU'] if row['necessidadeSKU'] <= row[col_name2] and row["Necessidade Endereco"]  ==0 else row["Necessidade Endereco"] , axis=1)

        pedidos["Endereco official"] = pedidos.apply(
            lambda row: row[col_name] if row['Necessidade Endereco'] >0 and row["Endereco official"]  =='%'   else '%' , axis=1)

       # pedidos["necessidade"] = pedidos.apply(
        #    lambda row: row['necessidade']-row["Necessidade Endereco"]  if row['necessidade'] > 0 else 0 , axis=1)

       # pedidos['necessidadeSKU'] = pedidos.groupby('codreduzido')['necessidade'].cumsum()

        #pedidos['Endereço Oficial'] = if


    # Replicando a regra para os proximos endereços.
    pedidos.to_csv('necessidade.csv')
    estoque.to_csv('estoque.csv')
    return print(pedidos)

def ApontamentoTagSeparacao():

    return 'API de apontar as tags separadas'



def DetalhaPedido(codPedido):
    conn = ConexaoPostgreRailway.conexao()
    skus = pd.read_sql('select codigopedido, desc_tiponota  , codcliente ||'+"'-'"+'|| desc_cliente as cliente  '
                                ',codrepresentante  ||'+"'-'"+'|| desc_representante  as repres '
                                'from "Reposicao".filaseparacaopedidos f  where codigopedido= '+"'"+codPedido+"'"
                                ,conn)
    DetalhaSku = pd.read_sql('select  produto as reduzido, qtdesugerida , status as concluido_X_total, enderco as endereco'
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
