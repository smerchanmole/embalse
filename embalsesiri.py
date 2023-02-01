# librerias
from datetime import datetime
# importamos la libreria de conexion a postgres. Antes hay que instalarla con pip3 install psycopg2-binary
import psycopg2
import sys #para coger los argumentos
from connections import * #para coger los datos de conexion y las claves

# #########################
# FUNCIONES PARA ACCESO A BBDD Y LOGS
# #########################
# La funciona de conexión a la base de datos para peticiones
def conectar_bd (PS_HOST, PS_PORT, PS_USER, PS_PASS, PS_DB, PS_QUERY):
    #"""Funcion para conectar con la base de datos, mandamos los datos de conexion y la consulta,
    #devolvemos un array con cursor y connector"""
    #realizamos la conexión
    try:
        # Conectarse a la base de datos
        connstr = "host=%s port=%s user=%s password=%s dbname=%s" % (PS_HOST, PS_PORT, PS_USER, PS_PASS, PS_DB)
        conn = psycopg2.connect(connstr)

        # Abrir un cursor para realizar operaciones sobre la base de datos
        cur = conn.cursor()
        
        #Ejecutamos la peticion
        cur.execute(PS_QUERY)

        
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

def consulta_bbdd (sql_query):
    #conectamos a bbdd
    cur,con = conectar_bd (database_ip,database_port,database_user,database_password, database_db,"select 1" ) 

    cur.execute(sql_query)
    datos=cur.fetchall()
    return datos
def coger_ultimo_peso ():
    #conectamos a bbdd
    cur,con = conectar_bd (database_ip,database_port,database_user,database_password, database_db,"select 1" ) 
    # escribimos
    Query="SELECT fecha, valor from PESO order by fecha desc limit 1"

    cur.execute(Query)
    datos=cur.fetchall()
    peso=datos[0][1]
    fecha=datos[0][0]
    texto="tu peso fue de " + str(peso)
    texto+=" el dia "+ fecha.strftime("%d")+ " de "+ fecha.strftime("%B")
    #print (texto)
    #y cerramos, asi evitamos time out.
    cerrar_conexion_bbdd (cur,con)
    return (peso, texto)
#############################################
## MAIN
#############################################

query="SELECT public.embalse.volumen/91*100 AS porcentaje, fecha \
       FROM public.embalse \
       order by fecha desc \
       LIMIT 2  "
res=consulta_bbdd (query)

porcentaje=res[0][0]
fecha=res[0][1]
ultimo_dia=int(fecha.strftime("%d"))
dif_dias=res[0][1]-res[1][1]
dif_porcentaje=res[0][0]-res[1][0]

print ('El embalse está al %.2f' % porcentaje,'por ciento a día ',ultimo_dia,'. ')

if dif_porcentaje>0:
    print ("Es un %.2f" % dif_porcentaje,"por ciento más que hace",dif_dias.days,"días.")
else:
    print ("Es un %.2f" % dif_porcentaje," por ciento menos que hace",dif_dias.days, "días.")

# print (texto_salida)



