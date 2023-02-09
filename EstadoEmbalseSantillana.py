# ####################################################################### #
# APP: Web scrapper to fetch reservoir water capacity                     #
# Reservoir name: Embalse de Santillana                                   #
# Year:2020                                                               #
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

# ####################################################################### #
# Data Base functions to avoid complex main program reading.              #
# ####################################################################### #


# ####################################################################### #
# FUNTION: conectar_db                                                    #
# DESCRIPTION: Generate a connection to the database (postgreSQL)         #
# INPUT: Data needed to connect and the inital connection query           #
# OUTPUT: Cursor and Connection,  print error if happens                  #
# ####################################################################### #
def conectar_bd (PS_HOST, PS_PORT, PS_USER, PS_PASS, PS_DB, PS_QUERY):
    try:
        connstr = "host=%s port=%s user=%s password=%s dbname=%s" % (PS_HOST, PS_PORT, PS_USER, PS_PASS, PS_DB)
        conn = psycopg2.connect(connstr)

        # Open the cursor and launch initial query
        cur = conn.cursor()
        
        # Query execution
        cur.execute(PS_QUERY)
        
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        if conn is not None:
            conn.close()
            print('Database connection closed.')   
            cur=""
            conn=""

    return cur, conn

# ####################################################################### #
# FUNTION: cerrar_conexion_bbdd                                           #
# DESCRIPTION: Close the connection                                       #
# INPUT: Data needed to close                                             #
# OUTPUT: Nothing                                                         #
# ####################################################################### #
def cerrar_conexion_bbdd (PS_CURSOR, PS_CONN):
    PS_CURSOR.close()
    PS_CONN.close()

# ####################################################################### #
# FUNTION: escribir_log                                                   #
# DESCRIPTION: Write the operation in a log table (just for info)         #
# INPUT: Data needed to write the log                                     #
# OUTPUT: 1                                                               #
# ####################################################################### #
def escribir_log (PS_CURSOR, PS_CONN, ip, comando, extra):
    # Escribimos el mensaje en la tabla logs. 
    x=datetime.datetime.now()
    # x.isoformat() para tener el timestamp formato ISO
    InsertLOG="INSERT INTO public.logs (hora, ip, comando, extra) values ('"+str(x.isoformat())+"','"+ip+"','"+comando+"','"+extra+"')"
    # print (InsertLOG)

    PS_CURSOR.execute(InsertLOG)
    return 1


# ####################################################################### #
# ####################################################################### #
# ####################################################################### #
# MAIN                                                                    #
# ####################################################################### #
# ####################################################################### #
# ####################################################################### #

#Here is the URI where the data of the reservoir is noted.
uri_total= "https://www.embalses.net/pantano-1013-santillana.html"
print(uri_total)


# Make the request and save the html response.
response=requests.get(uri_total, verify=False)
print(response)

# Show the html code 
response.text


#Search the part of the page where the data is printed
indice=response.text.find('Campo"><strong>Agua embalsada')
print (indice)

#let's fetch 300 chars to look deeper
crawler=response.text[indice:indice+300]
print (crawler)

#Now we take the date from the html code
indice_fecha=crawler.find("strong")
print (indice_fecha)
fecha=crawler[indice_fecha+23: indice_fecha+23+10]
print (fecha)

#format date in postgreSQL and forget the time
anio=fecha[6:10]
mes=fecha[3:5]
dia=fecha[0:2]
print ("dia",dia,"mes",mes,"anio",anio)
fecha=anio+"/"+mes+"/"+dia
print (fecha)

#Fetch the volume  
#1st remove the date
volumen=crawler[indice_fecha+23+10+10:indice_fecha+23+10+100]
#print (volumen)
indice_volumen=volumen.find("strong")
volumen=volumen[indice_volumen+7:indice_volumen+7+5]
print(volumen)


#Fetch the numbers 
print(volumen)
lista_numeros_cogidos=[float(s) for s in re.findall(r'-?\d+\.?\d*',volumen)]

volumen=lista_numeros_cogidos[0]
volumen=str(volumen)
print (volumen)

# Write date and volume in postgreSQL database
SQLupsert="insert into public.embalse (fecha, volumen) "
SQLupsert+="values ('"+fecha+"',"+volumen+") on conflict(fecha) do nothing"

#Now we generate the connection
cur,con = conectar_bd (database_ip,database_port,database_user,database_password, database_db,"select count(*) from public.sonda_colmenar")
print(SQLupsert)

# Execute the upsert
cur.execute(SQLupsert)

#Update the logs
   
#al final del for actualizamos logs
escribir_log(cur, con, "192.168.1.11","ACTUALIZAMOS embalse","operacion normal")

# Commit the data on database (just to make sure)
con.commit()

# Close the connection
cerrar_conexion_bbdd (cur,con)