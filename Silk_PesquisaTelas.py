import psycopg2
import ConecaoAWSRS


def PesquisaEnderecos (Coluna,Operador,Nome):
    conn = ConecaoAWSRS.conexao()

    consulta = f'select "Referencia", "Endereco" from "silkMPL"."enderecamento"  WHERE "{Coluna}" {Operador} %s'
    valor = (Nome,)
    cursor = conn.cursor()
    cursor.execute(consulta, valor)
    resultados = []
    for resultado in cursor.fetchall():
        resultados.append(resultado)

    cursor.close()
    conn.close()
    print('Consulta Retornada com Suecesso')
    return resultados


resultados = PesquisaEnderecos("Referencia", "=", '45005')
print(resultados)


def Funcao_Deletar (Endereco,Produto):

    conn = psycopg2.connect(host="localhost", database="postgres", user="postgres", password="master100")

    cursor =conn.cursor()

    sql= 'DELETE FROM "SchemaProdutividadeMPL"."Enderecamento" where "Endereco" = %s and "Referencia" = %s  '
    VALORES = (Endereco, Produto,)
    cursor.execute(sql, VALORES)
    conn.commit()
    cursor.close()

    conn.close()
#Funcao_Deletar_Tamanhos("12")

def Funcao_Inserir (Referencia, Endereco):

    conn =psycopg2.connect(host="localhost",database="postgres",user="postgres",password="master100")

    cursor =conn.cursor()

    sql= 'INSERT INTO "SchemaProdutividadeMPL"."Enderecamento" ("Referencia", "Endereco") VALUES (%s, %s)'
    VALORES = (Referencia, Endereco,)
    cursor.execute(sql, VALORES)
    conn.commit()
    cursor.close()

    conn.close()
    # return Funcao_Inserir



#Funcao_InserirTamanhos(2, "Calça Legging", "Calça", "Ativo", "15/05/2023", "joao.ferreira")
