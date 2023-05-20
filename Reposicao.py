import pandas as pd
import ConecaoAWSRS
import numpy
# CLASSE COM AS FUNÇOES PARA INTERAGIR COM AS APIS DE ACESSO DA "REPOSICAO"

def ObeterEnderecos():
    conn = ConecaoAWSRS.conexao()
    endercos = pd.read_sql(
        ' select * from "Reposicao"."cadEndereco" ce   ', conn)
    return endercos

