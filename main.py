# Comienzo del Código # -*- coding: utf-8 -*-

import psycopg2
import psycopg2.extras
import sys
import pprint  # Para mostrar los valores de las tuplas recibidas

# Conexión a la base de datos en Postgresql
print()
print("PRUEBA DE CREACIÓN DE TABLAS EN POSTGRES Y REALIZACIÓN DE CONSULTAS")
print()

conx = None  # Para destruir cualquier conexión conx existente
print("Conexión a la Base de Datos Postgres")

# Se usa try para poder capturar las excepciones producidas durante la conexión

try:
    # Se realiza la conexión con la base de datos postgres

    conx = psycopg2.connect("dbname=postgres user=postgres password=postgres")
    print("Estableciendo conexión a la base de datos ...")
    # conx.cursor devuelve un objeto cursor necesario para realizar las consultas SQL
    cur = conx.cursor()
    print("Conectado!\n")

    # Se ejecutan una serie de consultas o queries

    # Si existe la tabla prueba se borrará para evitar excepciones al ejecutar el código de nuevo

    cur.execute("DROP TABLE IF EXISTS prueba")
    print("La tabla prueba se ha eliminado")

    # Se crea una tabla nueva llamada prueba con un campo que será clave primaria

    cur.execute(
        "CREATE TABLE prueba (id serial PRIMARY KEY, nombre varchar, sueldo integer)"
    )

    # Se insertan algunas tuplas en la tabla. La última se inserta de otra forma

    cur.execute(
        "INSERT INTO prueba (nombre, sueldo) VALUES (%s, %s)", ("Miguel Sánchez", 1500)
    )
    cur.execute(
        "INSERT INTO prueba (nombre, sueldo) VALUES (%s, %s)", ("Clara Muñoz", 1600)
    )
    cur.execute(
        "INSERT INTO prueba (nombre, sueldo) VALUES (%s, %s)", ("Paco López", 1580)
    )
    cur.execute(
        "INSERT INTO prueba (nombre, sueldo) VALUES (%s, %s)", ("Nerea Montalbán", 1570)
    )
    cur.execute(
        "INSERT INTO prueba (nombre, sueldo) VALUES (%s, %s)", ("Luis Bermúdez", 1650)
    )
    cur.execute(
        "INSERT INTO prueba (nombre, sueldo) VALUES (%s, %s)", ("Ana Palomo", 1590)
    )

    # Se inserta una tupla más de otra manera
    cur.execute(
        """INSERT INTO prueba (nombre, sueldo)  
        VALUES (%s, %s);""",
        ("Lola Otero", 1700),
    )

    ############# ---------------- ################### CONSULTAS #####################
    ############# CONSULTAS ################### ---------------- #####################
    ############# ---------------- ################### CONSULTAS #####################

    # Se realiza una consulta para comprobar si se han introducido dichas tuplas

    cur.execute("SELECT * FROM prueba")
    tuplas = cur.fetchall()
    print()

    print("Se muestran todas las tuplas con un bucle for")
    for row in tuplas:
        print(row)

        # Se muestran las tuplas mediante pprint

    print()
    print("Se muestran todas las tuplas usando pprint")
    pprint.pprint(tuplas)

    # Se ven las tuplas una a una y se muestran los campos deseados de las mismas

    cur.execute("SELECT * FROM prueba")
    print()
    print("Se muestran todos los campos de la fila:")
    while True:
        fila = cur.fetchone()  # Sólo se extrae una tupla del cursor
        if fila == None:  # Si no hay más filas se sale del bucle while
            break
        print(
            fila[0], fila[1], fila[2]
        )  # para la fila, se muestran los valores de los campos 0, 1 y 2
    print()

    # Se muestra la tupla que cumple una condición

    print("Se muestra sólo la tupla que cumpla la condición - nombre = Luis")
    sql = "select * from prueba where nombre like 'Luis%'"
    cur.execute(sql)
    fila = cur.fetchone()
    print(fila)

    # Se añade un campo nuevo a la tabla

    cur.execute("alter table prueba add column telefono integer")

    # Se introducen los valores en el nuevo campo

    cur.execute("update prueba set telefono=911111111 where id=1")
    cur.execute("update prueba set telefono=912222222 where id=2")
    cur.execute("update prueba set telefono=913333333 where id=3")
    cur.execute("update prueba set telefono=914444444 where id=4")
    cur.execute("update prueba set telefono=915555555 where id=5")
    cur.execute("update prueba set telefono=916666666 where id=6")
    cur.execute("update prueba set telefono=917777777 where id=7")

    # Se realiza una consulta para comprobar los cambios realizados

    cur.execute("SELECT * FROM prueba")
    tuplas = cur.fetchall()  # Se extraen todas las tuplas del cursor

    print()
    print("Se muestran todas las tuplas con un bucle for")
    for row in tuplas:
        print(row)

        # Se muestra solo la información de un campo

    print()
    cur.execute("SELECT nombre, sueldo FROM prueba")
    print("Nombres:")
    for nombre, sueldo in cur.fetchall():
        print(nombre)

        ##########################################################################

    # Confirmar cambios y cerrar

    conx.commit()  # Confirmar cambios y hacerlos permanentes

    cur.close()  # Se cierra el cursor
    conx.close()  # Se cierra la conexión
    print("La conexión con la base de datos se ha cerrado")

except:

    print("No se puede conectar con la Base de Datos")

# Fin del código
