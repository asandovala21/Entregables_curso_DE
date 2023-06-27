# !python3 -m pip install psycopg2
import requests
import pandas as pd
from pandas import json_normalize 
import json
import psycopg2
from psycopg2.extras import execute_values
import missingno as msno
import numpy as np

def cargar_en_redshift(user, pwd, table_name, dataframe):
    data_base = "data-engineer-database"
    try:
        conn = psycopg2.connect(
            host='data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com',
            dbname=data_base,
            user=user,
            password=pwd,
            port='5439'
        )
        print("Conexión a Redshift exitosa!")

    except Exception as e:
        print("No se pudo conectar a Redshift.")
        print(e)
    
    
    dtypes = dataframe.dtypes
    cols = list(dtypes.index)
    tipos = list(dtypes.values)
    type_map = {'int64': 'INT', 'int32': 'INT', 'float64': 'FLOAT',
                'object': 'VARCHAR(50)', 'bool': 'BOOLEAN', 'datetime64[ns, UTC]': 'TIMESTAMP'}
    sql_dtypes = [type_map[str(dtype)] for dtype in tipos]
    sql_dtypes = [x.replace('VARCHAR(50)', f'VARCHAR({len(max(dataframe[y].fillna(""),key=len))})')
                  if x == 'VARCHAR(50)' else x for x, y in zip(sql_dtypes, cols)]
    # Definir formato SQL VARIABLE TIPO_DATO
    column_defs = [f"{name} {data_type}" for name,
                   data_type in zip(cols, sql_dtypes)]
    name=cols[0]
    primary_key=[f"primary key({name})"]
    column_defs.append(primary_key[0])
    # Combine column definitions into the CREATE TABLE statement
    table_schema = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join(column_defs)}
        );
        """
    # Crear la tabla
    cur = conn.cursor()
    cur.execute(table_schema)
    # Generar los valores a insertar
    values = [tuple(x) for x in dataframe.to_numpy()]
    
    # for i in range(4):
    #     print(f'Valores de {i}')
    #     for j in range(len(values)):
    #         print(len(values[j][i]))
        
    # Definir el INSERT
    insert_sql = f"INSERT INTO {table_name} ({', '.join(cols)}) VALUES %s"
    # Execute the transaction to insert the data
    cur.execute("BEGIN")
    execute_values(cur, insert_sql, values)
    cur.execute("COMMIT")
    print('Proceso terminado')

#CAT Facts
url = "https://cat-fact.herokuapp.com/facts"
headers = {"Accept-Encoding": "gzip, deflate"}

response = requests.get(url, headers=headers)
data = response.json()
data
ids= [data[x].get('_id')
           for x in range(len(data))]
texts = [data[x].get('text')
                  for x in range(len(data))]
updatedAt = [data[x].get('updatedAt')
               for x in range(len(data))]
types = [data[x].get('type')
                     for x in range(len(data))]
createdAt = [data[x].get('createdAt')
                     for x in range(len(data))]
used = [data[x].get('used')
                     for x in range(len(data))]

df = pd.DataFrame({'Id': ids, 'Text': texts,
                  'UpdatedAt': updatedAt, 'types': types, 'CreatedAt': createdAt, 'Used': used})

#Revisar los tipos de datos
df.dtypes
#Cambiar el tipo de dato de la columna 'UpdatedAt' a datetime
df['UpdatedAt'] = pd.to_datetime(df['UpdatedAt'], dayfirst=False)
#Cambiar el tipo de dato de la columna 'CreatedAt' a datetime
df['CreatedAt'] = pd.to_datetime(df['CreatedAt'], dayfirst=False)
#Revisar si hay missing
msno.matrix(df)
#Revisar si hay duplicados
df.duplicated().sum()
#Revisar otra vez si hay nulos
df.isnull().sum()
df.info()
df.describe()
df = df.drop_duplicates()
df['UltimaActualizacion'] = df['UpdatedAt'].max()
df['UltimaCreación'] = df['CreatedAt'].max()

df_2=df.groupby('types').agg(ultima_fecha_creacion=('CreatedAt',np.max), ultima_fecha_actualizacion=('UpdatedAt', np.max)).reset_index()
user = "asandovala_coderhouse"
pwd = "4QWq7d0OSV"
#CARGANDO DATOS EN REDSHIFT

cargar_en_redshift(
    user=user, pwd=pwd, table_name='base_cats', dataframe=df)
cargar_en_redshift(
    user=user, pwd=pwd, table_name='base_cats_resumen', dataframe=df_2)