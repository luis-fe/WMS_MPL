import pandas as pd

# Crie um DataFrame de exemplo
data = {'agrupamento': ['ab/xxx/yyy', 'ccc/dd', 'dddd']}
df = pd.DataFrame(data)

# Crie uma nova coluna chamada 'desagrupado'
df['desagrupado'] = df['agrupamento'].apply(lambda x: x.split('/')[1:] if '/' in x else None)

# Exiba o novo DataFrame
print(df)
