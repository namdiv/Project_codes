# %% [markdown]
# # Proyecto Grupal - Etapa 04: Limpieza de Datos
# Autores:
# Camila de la Paz
# Daniel E. Ramírez
# Franco Pes
# Xavier Vidman
# 
# El presente archivo compila la tercera etapa del primer proyecto grupal de la carrera de Data Science de Henry, un análisis exploratorio sobre los datos proporcionados. Esta etapa se divide, a su vez, en los pasos que se detallan a continuación:
# 1. Importación de liberías a utilizar
# 2. Carga de datos
# 3. Limpieza y Transformación

# %% [markdown]
# ### Paso 1: Importación de librerías

# %%
import datetime 
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
sns.set(style="whitegrid", color_codes=True)

# %% [markdown]
# ### Paso 2: Carga de datos
# 

# %%
df = pd.read_parquet ('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2018-01.parquet')

# %% [markdown]
# ### Paso 3: Limpieza y Transformación

# %% [markdown]
# #### a) Se eliminaron columnas innecesarias

# %%
df.drop(columns=['congestion_surcharge','airport_fee'],inplace=True)

# %% [markdown]
# #### b) Se cambiaron valores negativos que se esperaban positivos

# %%
df.fare_amount = df.fare_amount.abs()
df.extra = df.extra.abs()
df.mta_tax = df.mta_tax.abs()
df.tip_amount = df.tip_amount.abs()
df.tolls_amount = df.tolls_amount.abs()
df.improvement_surcharge = df.improvement_surcharge.abs()
df.total_amount = df.total_amount.abs()

# %% [markdown]
# #### c) Se eliminaron registros duplicados

# %%
df[df.duplicated()]

# %%
df.drop_duplicates(inplace=True)

# %% [markdown]
# #### d) Se eliminaron dos datos erróneos

# %%
df[df.trip_distance > 500]

# %%
df.drop([1858262,8237763],inplace=True)

# %% [markdown]
# #### e) Se creó una nueva columna, fare_per_mile, para estudiar la relación entre fare_amount y trip_distance

# %%
df['fare_per_mile'] = df.fare_amount / df.trip_distance

# %% [markdown]
# Debido que algunos registros tenían trip_distance 0, el fare_per_mile es infinito o NaN. Se les asignó 0 a estos registros

# %%
df.fare_per_mile[df.fare_per_mile > 100000].unique()

# %%
df[df.fare_per_mile.isna()].count()

# %%
df.fare_per_mile[df.fare_per_mile > 100000] = 0

# %%
df.fare_per_mile.fillna(0,inplace=True)

# %% [markdown]
# #### f) Se creó una nueva columna, trip_time, para identificar el tiempo de viaje en segundos

# %% [markdown]
# Primero se calculó la diferencia de tiempo

# %%
df['trip_time'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime

# %% [markdown]
# Luego se convirtió a segundos

# %%
df.trip_time = df.trip_time.dt.total_seconds()

# %%
df[df.trip_time < 0]

# %%
df.drop([158888,6146145],inplace=True)

# %% [markdown]
# #### g) Se creó una nueva columna, fare_per_minute, para identificar la relación entre el fare_amount y el trip_time

# %%
df['fare_per_minute'] = df.fare_amount / (df.trip_time / 60)

# %%
df.fare_per_minute.describe()

# %%
df.fare_per_minute[df.fare_per_minute > 100000].unique()

# %%
df.fare_per_minute[df.fare_per_minute > 100000] = 0

# %%
df[df.fare_per_minute.isna()].count()

# %%
df.fare_per_minute.fillna(0,inplace=True)

# %% [markdown]
# #### h) Se creó una nueva columna, id_borough, para identificar a qué Borough pertenece cada viaje

# %% [markdown]
# Lo primero es cargar el dataframe con las zonas de taxis

# %%
df_zones = pd.read_csv('taxi+_zone_lookup.csv')

# %%
df_zones.info()

# %%
df_zones.Borough.unique()

# %% [markdown]
# Se reemplazaron los nombres de los boroughs por id's

# %%
df_zones.Borough.replace({"Bronx":0, "Brooklyn":1, "Manhattan":2, "Queens":3, "Staten Island":4, "EWR":5, "Unknown":6}, inplace=True)

# %% [markdown]
# Se creó un diccionario de zonas con su respectivo borough_id para luego mapear

# %%
dic_zone_borough = {df_zones.LocationID[i] : df_zones.Borough[i] for i in range (0,len(df_zones))}

# %% [markdown]
# Se creó una nueva columna con su respectivo id_borough

# %%
df['id_borough'] = df.PULocationID.map(dic_zone_borough)

# %% [markdown]
# #### i) Se creó una nueva columna, id_time_borough, para posteriormente relacionar con la tabla de daots climáticos

# %%
df['id_time_borough'] = df.tpep_pickup_datetime.dt.strftime('%Y%m%d%H') + df.id_borough.astype(str)

# %% [markdown]
# #### j) Trimming de los datos
# Durante la etapa anterior, en el análisis exploratorio, se detectó una cantidad importante de outliers en los distintos campos que conforman el dataset. Se consideró, para la carga inicial de los datos en el Data Warehouse, realizar trimming de los datos, esto es, descartar una parte de los datos localizados en los extremos de su distribución, los menos frecuentes. Se evaluaron distintas alternativas para realizar el trimmig, pero se decidió descartar aquellos registros donde fare_amount (tarifa base) es menor al cuartil 5% y mayor al cuartil 95%.

# %%
df_final = df[(df.fare_amount > df.fare_amount.quantile(.05)) & (df.fare_amount < df.fare_amount.quantile(.95))]


import pymysql
connection = pymysql.connect{
    host='hpsw9l5rkyvw.us-east-1.psdb.cloud',
    user='pg3cl2vffprf',
    passwd='',
    database='taxis_nyc'
    ssl = {'ca': '/etc/ssl//certs/ca-certificates.crt'}

}

mycursor = connection.cursor()


for i in df_final.index.values:
    v = df_final.iloc[i].tolist()    
    mycursor.execute(f'INSERT INTO taxi (index, VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_max, tip_amount, improvement_surcharge, total_amount, fare_per_mile, trip_time, fare_per_minute) VALUES {v}')

