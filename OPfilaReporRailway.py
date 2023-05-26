def ProdutividadeRepositores():
    conn = ConexaoPostgreRailway.conexao()
    cursor = conn.cursor()
    cursor.execute('select tr."Usuario", '
                   'count(tr."codbarrastag"), '
                   'substring(tr."DataReposicao",1,10) as DataReposicao '
                   'from "Reposicao"."tagsreposicao" tr '
                   'group by "Usuario" ,substring("DataReposicao",1,10)')
    TagReposicao = cursor.fetchall()
    return TagReposicao
