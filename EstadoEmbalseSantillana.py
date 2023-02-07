# ####################################################################### #
# APP: Web scrapper to fetch reservoir water capacity                     #
# Reservoir name: Embalse de Santillana                                   #
#                                                                         #
#                                                                         #
# ####################################################################### #

# import pandas to manage data, request & re  to de the scrapping, json to 
# manage the jsons and sys to load a file with the passwords, psycopg2 to 
# load data to postgress database
import pandas as pd
import requests
import json
import time
import datetime
import re
import psycopg2 #to install with pip3 install psycopg2-binary

# Load data from parent directory (that is why we need sys)
sys.path.append ('..')
from connections import *


# ## Funciones para el acceso a base de datos

# La funciona de conexión a la base de datos para peticiones
def conectar_bd (PS_HOST, PS_PORT, PS_USER, PS_PASS, PS_DB, PS_QUERY):
    """Funcion para conectar con la base de datos, mandamos los datos de conexion y la consulta,
    devolvemos un array con cursor y connector"""
    #realizamos la conexión
    try:
        # Conectarse a la base de datos
        connstr = "host=%s port=%s user=%s password=%s dbname=%s" % (PS_HOST, PS_PORT, PS_USER, PS_PASS, PS_DB)
        conn = psycopg2.connect(connstr)

        # Abrir un cursor para realizar operaciones sobre la base de datos
        cur = conn.cursor()
        
        #Ejecutamos la peticion
        cur.execute(PS_QUERY)

        # Obtener los resultados como objetos Python
        row = cur.fetchone()
        print (row)
        # Recuperar datos del objeto Python
        #username = row[1]

        
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        if conn is not None:
            conn.close()
            print('Database connection closed.')   
    return cur, conn

def cerrar_conexion_bbdd (PS_CURSOR, PS_CONN):
    # Cerrar la conexión con la base de datos
    PS_CURSOR.close()
    PS_CONN.close()

def escribir_log (PS_CURSOR, PS_CONN, ip, comando, extra):
    # Escribimos el mensaje en la tabla logs. 
    x=datetime.datetime.now()
    # x.isoformat() para tener el timestamp formato ISO
    InsertLOG="INSERT INTO public.logs (hora, ip, comando, extra) values ('"+str(x.isoformat())+"','"+ip+"','"+comando+"','"+extra+"')"
    # print (InsertLOG)

    PS_CURSOR.execute(InsertLOG)
    return 1

    


# ## Acceso a datos web de embalset net via crawler
# #### buscamos los datos y hora de la ultima actualizacion
# 

# In[7]:



uri_total= "https://www.embalses.net/pantano-1013-santillana.html"
print(uri_total)


# In[10]:


#hacemos la llamada y capturamos los datos en un objeto response, si lo imprimimos nos dice el resultado de HTTP
response=requests.get(uri_total, verify=False)
print(response)


# In[11]:


# Si llamamamos al procedimiento .text nos da la salida 
response.text


# In[17]:


#buscammos donde empiezan los datos
indice=response.text.find('Campo"><strong>Agua embalsada')
print (indice)


# In[18]:


#cogemos 300 caracteres
crawler=response.text[indice:indice+300]
print (crawler)


# In[22]:


#cogemos la fecha
indice_fecha=crawler.find("strong")
print (indice_fecha)
fecha=crawler[indice_fecha+23: indice_fecha+23+10]
print (fecha)


# In[23]:


#vamos a poner la fecha en formato postgre y dejar la hora
anio=fecha[6:10]
mes=fecha[3:5]
dia=fecha[0:2]
print ("dia",dia,"mes",mes,"anio",anio)
fecha=anio+"/"+mes+"/"+dia
print (fecha)


# In[43]:


#cogemos el volumen  

#primero quitamos la parte de la fecha
volumen=crawler[indice_fecha+23+10+10:indice_fecha+23+10+100]
#print (volumen)
indice_volumen=volumen.find("strong")
volumen=volumen[indice_volumen+7:indice_volumen+7+5]
print(volumen)


# In[44]:


#Cogemos el numero que quede en el texto
print(volumen)
lista_numeros_cogidos=[float(s) for s in re.findall(r'-?\d+\.?\d*',volumen)]

volumen=lista_numeros_cogidos[0]
volumen=str(volumen)
print (volumen)


# ## Metermos los datos en postgre SQL.
# #### Como puede haber datos repetidos por horas, hacemos un "upsert"
# 

# In[45]:


##insertamos las ultimas 24 horas en la tabla con un UPSERT 



# In[46]:


#definimos la operacion SQL
SQLupsert="insert into public.embalse            (fecha, volumen)            values ('"+fecha+"',"+volumen+") on conflict(fecha) do nothing"

#conectamos con la tabla
cur,con = conectar_bd (database_ip,database_port,database_user,database_password, database_db,"select count(*) from public.sonda_colmenar")
print(SQLupsert)

    


# In[47]:




cur.execute(SQLupsert)
   

    
#al final del for actualizamos logs
escribir_log(cur, con, "192.168.1.11","ACTUALIZAMOS embalse","operacion normal")


# In[48]:


#con.rollback() #por si da error de current transaccion is aborted, commands ignored until end of trans


# In[49]:


con.commit() #hay que hacer esto para que se escriban


# In[50]:


##cerramos la conexion
cerrar_conexion_bbdd (cur,con)


# In[ ]:





# In[ ]:




