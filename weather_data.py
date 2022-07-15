#cada tupla tiede: latitud, longitud, elevacion, nombre del boroug
bronx = [40.8499, -73.8664, 19, "The Bronx"]
brooklyn = [40.6501, -73.9496, 18, 'Brooklyn']
manhattan = [40.7834, -73.9663, 38, 'Manhattan']
queens = [40.6815, -73.8365, 13, 'Queens']
staten_island = [40.5623, -74.1399, 23, 'Staten Island']

#generamos una lista  con las tuplas
lista = [bronx, brooklyn, manhattan, queens,staten_island]

#importamos las librerias 
from datetime import datetime
import matplotlib.pyplot as plt
#PIP INSTALL METEOSTAT
from meteostat import Point, Daily, Hourly
import pandas as pd

#descarga de la informacion 
#descargamos la info de todos los boroughs en un mismo dataframe

df_list = []
for i in lista:
    #Seteo del periodo estudiado
    start = datetime(2018, 1, 1)
    end = datetime(2018, 1, 31)
    #seteo de locacion (latitud, longitud, elevacion)
    location = Point([i][0][0], [i][0][1], [i][0][2])
    # indicamos que queremos una frecuencia por hora
    data = Hourly(location, start, end)
    data = data.fetch()
    #creamos un dataframe
    df = pd.DataFrame(data)
    df['Borough'] = [i][0][3]
    df_list.append(df)
df = pd.concat(df_list) 

df.reset_index(inplace=True)

#eliminamos las columnas con valores muchos valores nulos (o todos)
df.drop(["snow"], axis = 1, inplace = True)
df.drop(["wpgt"], axis = 1, inplace = True)
df.drop(["tsun"], axis = 1, inplace = True)
df.drop(["coco"], axis = 1, inplace = True)

df['prcp'].fillna(0, inplace = True)

a = df['wdir'].mean() #promedio de direccion de viento
df['wdir'].fillna(a, inplace=True)
   
b = df['pres'].mean() #promedio de direccion de viento
df['pres'].fillna(b, inplace=True) #promedio de presion atmosferica
print(df.info())

df.rename(columns={"dwpt": "dew_point", "rhum": "hum", "prcp":"rain", "Borough":"borough", "wdir":"wind_dir", "wspd":"wind_speed"}, inplace=True)

df_borough = pd.DataFrame(df['borough'].unique())

df_borough.reset_index(inplace = True)

df_borough.rename(columns={0: "borough", "index":"id_borough"}, inplace=True)

df.replace({"The Bronx":0, "Brooklyn":1, "Manhattan":2, "Queens":3, "Staten Island":4}, inplace=True)

df.rename(columns={"borough":"id_borough"}, inplace=True)

df.loc[df.rain > 20, 'rain'] = 1

df['id_time_borough'] = df.time.dt.strftime('%Y%m%d%H') + df.id_borough.astype(str)

###############################################################33
# tablas----> df y df_borough
# queda hacer la conexion