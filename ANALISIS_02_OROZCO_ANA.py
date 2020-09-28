#!/usr/bin/env python
# coding: utf-8

# # Prueba de que spark funciona en Jupyter

# In[1]:


#Para el analisis de cada consigna se opto por utilizar la libreria de SPARK y Pandas para el desarrollo y obtencion de resultados
import pyspark
#En este caso importamos la libreria de spark que se usa para manejar grandes cantidades de datos
import pandas as pd
#en el caso de pandas, se usara para uno que otro movimiento de analisis
#import matplotlib.pyplot as plt para poder hacer las graficas de los datos si es necesario usarse.


# In[2]:


from pyspark.sql import SparkSession
#En esta sentencia se importa la herramienta de sesion donde se almacenara el desarrollo del codigo


# In[3]:


spark = SparkSession.builder.appName("SynergyLogistics").getOrCreate()
#En la linea de codigo se habla sobre dar el nombre y crear la sesion que se almacenara en la variable de spark


# In[4]:


from pyspark.sql import *
#asi mismo importamos todas las herramientas de sql para manejarlo


# In[5]:


df=spark.read.csv('synergy_logistics_database.csv', inferSchema=True, header=True)
#en esta funcion se pasa el csv a data frame para un mejor manejo


# In[6]:


df.count()
#Aqui contamos cuantos registros tiene nuestro data frame, solo para darnos una idea de cuanto es de informacion


# In[7]:


df.dropna(how='any').count()
#con esta sentencia observamos si la fuente de datos cuenta con datos nulos o vacios, para que en caso de que si cuente con alguno, lo borre para una mejor presicion


# In[8]:


df.printSchema()
#con esta funcion podemos sacar el tipo de dato de cada variable, asi mismo si permite datos nulos o no


# In[9]:


df.columns
#si quisieramos que nos mostrara las columnas se utiliza esta sentencia


# In[10]:


df.show(5)
#Aqui para observar la informacion le ponemos que nos muestre 5 registros


# In[11]:


df.createOrReplaceTempView("SLogistics")
#creamos una  imagen en donde desarrollaremos las diferentes opciones del proyecto


# # Opcion 2 - Medio de transporte utilizado.

# In[12]:


#En este caso el uso de la herramienta de spark nos brinda diferentes Funciones que nos hara desarrollarlo mas sencillo


# In[13]:


#En esta linea almacenamos en la variable UsoTransporte, el obtener la frecuencia de que se repite el transporte
#el calcularlo con la funcion groupBy es para que nos tome solo los valores unicos en este caso hay 4 tipos de transporte que es el de mar, por tren, por tierra y por aire.
#y con la funcion count() contamos las veces que se repite cada transporte
UsoTransporte=df.groupBy("transport_mode").count()


# In[14]:


#aqui en esta linea acomodamos de forma descendente o sea de mayor a menor, los medios de transporte.
#para obtener los medios mas utilizados
UsoTransporte.sort("count",ascending=False).show()


# In[15]:


#En esta linea almacenamos en la variable transporteValor, el data frame agrupado por tipo de transporte con la suma del valor de cada exportacion e importacion realizada
transporteValor=df.groupBy("transport_mode").sum("total_value")


# In[16]:


#en este caso primero definimos que se realice la operacion y que se muestre para poder visualizarla
transporteValor.sort("sum(total_value)",ascending=False).show()
#en esta linea ya sabemos que salio bien nuestra instruccion y debemos almacenarla, y nos ayuda la funcion toPandas a un archivo csv para demostrar en algun reporte simplificado.
transporteValor.sort("sum(total_value)",ascending=False).toPandas().to_csv('Opcion2.csv')
#con esto obtenemos el punto dos que pedia en la consigna el medio de transporte utilizado


# In[17]:


#en el caso mio estaba observando que se pueden obtener mas informacion si seguimos trabajando con esta consigna
#Pues nos ayuda el hecho de que podemos hacer una separacion de que tipo de transporte se usa mas en las importaciones y exportaciones
#Obteniendo la frecuencia con la que se repite el medio de transporte con respecto a la direccion.
TransporteXDirection=df.groupBy("transport_mode","direction").count().orderBy("transport_mode",ascending=False)


# In[18]:


#con esta linea mostramos los tipos de transporte
TransporteXDirection.show()


# # Opcion 1 - Rutas de importación y exportación.

# In[19]:


#Si desearamos obtener las rutas mas demandantes clasificadas por exportacion e importacion, esto seria lo ideal
#pues demuestra cuales son las mas frecuentes a usar pero clasificadas por la direccion.
#sin embargo la consigna no define si importacion o exportacio seria lo ideal
df.groupBy("direction","origin","destination").count().orderBy("count",ascending=False).show()


# In[20]:


#en esta linea se busca saber cuantas importaciones y exportaciones se llevan hasta el momento
df.groupBy("direction").count().show()


# In[21]:


#en esta linea lo que se busca es obtener por medio del año de trabajo y de si fue exportación o importación
df.groupBy("year","direction").count().orderBy("direction","year").show()


# In[22]:


#en esta linea si se desea ejecutar se obtendra el agrupamiento por direccion, origen y destino, los 10 más frecuentes
#Sin embargo, queda a criterio.
#df.groupBy("direction","origin","destination").count().orderBy(["count",ascending=False],["direction"]).show(20)


# In[23]:


#los anteriores desarrollos son solo informacion posible que se pudiera obtener tomando solamente los datos que permite la opcion 1

#OPCION 1, en estas lineas se desarrolla la opcion 1
#Aqui se busca obtener la opcion 1 que es las rutas mas demandadas, independientemente de que sea exportacion o importacion
df.groupBy("origin","destination").count().orderBy("count",ascending=False).show(10)
df.groupBy("origin","destination").count().orderBy("count",ascending=False).toPandas().to_csv('Opcion1.csv')
#tambien se genera un archivo csv para manipulacion en futuro.


# # Opcion 3 - Valor total de importaciones y exportaciones.

# In[24]:


#aqui se busca obtener el valor de cada ruta, cual es la que cuenta con mayor valor monetario
#sin embargo aqui se define por ruta y en la consigna no lo pide por ruta si no en conjunto importación y exportación
df.groupBy("origin","destination").sum("total_value").orderBy("sum(total_value)",ascending=False).show(10)


# In[25]:


#OBTENEMOS EL TOTAL DE VALOR
df.groupBy().sum("total_value").show()


# In[26]:


#como se busca obtener el solo las que se pueda obtener el 80% se tomo en cuenta tomar las 10 países mas valiosas
#aqui sin importar que fueran importaciones o exportaciones se tomo en cuenta. Asi mismo sin que fuera por ruta como el ejemplo anterior

df.groupBy("origin",).sum("total_value").orderBy("sum(total_value)",ascending=False).show(10)
df.groupBy("origin",).sum("total_value").orderBy("sum(total_value)",ascending=False).toPandas().to_csv('Opcion3.csv')
#igual se genera un archivo csv para manejo futuro


# In[27]:


#Con esto ultimo podemos tener la forma de manipular los datos para obtener la ultima consigna que es el 80% de los paises mas valiosos
#que se encuentran en los 8 primeros
#los ultimos codigos se usan como referencia de lo que se puede obtener mas en esta consigna


# In[28]:


#aqui podemos obtener por año lo que se genero de valor monetario
#tomando en cuenta que sea agrupado por año y se sume con la funsion SUM
totalXaño=df.groupBy("year").sum("total_value").orderBy("year",ascending=False)


# In[29]:


#Aqui se muestra el resultado por año
totalXaño.show()


# In[32]:


print("Fin del comunicado")
spark.stop()


# In[ ]:




