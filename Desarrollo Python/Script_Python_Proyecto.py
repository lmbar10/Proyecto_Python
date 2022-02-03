                                                    ### PROYECTO PROCESAMIENTO BASES RENOVACION BANCO DEL NORTE ###
                                                    ###############################################################
                                                    
                                                                  ###VARIABLES DE CONEXION Y PAQUETES### 
--------------------------------------------------------------------------------------------------------------------------------------------------------------
##1. Importa los modulos de Pandas y Conexion BD
import pyodbc
import pandas as pd
import numpy as np

##2. Asignar variables para la conexion a BD
server = '(localdb)\MORFEO'
database = 'Seguros_BdN'
user = 'user_qa'
passw = 'qa2021seguros'

##3. Crear cadena de conexion y cursor
cnxn = pyodbc.connect('DRIVER=SQL Server Native Client 11.0;SERVER='+server+';DATABASE='+database+';UID='+user+';PWD='+ passw)
cursor = cnxn.cursor()

#4. Cargar archivo plano CSV a DatFrame
cosechas = pd.read_csv("Base_Seguros_Banco_del_Norte.csv", sep=";")
cosechas







