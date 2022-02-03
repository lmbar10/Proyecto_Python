                                                    ### PROYECTO PROCESAMIENTO BASES RENOVACION BANCO DEL NORTE ###
                                                    ###############################################################
                                                
                                                                        ###LISTADO DE FUNCIONES### 

--------------------------------------------------------------------------------------------------------------------------------------------------------------
#Limpieza de la Data Principal
def limpiar_columnas_csv(data):
    """Funcion que realiza la actualizacion de los nombres de las columnas del DatFrame para evitar errores en carga a BD"""
    data.replace({np.inf: np.nan, -np.inf: np.nan}, inplace=True)
    data= data.fillna(0)
    data = data.rename(columns = {'ANIO COSECHA':'anio_cosecha', 'MES COSECHA':'mes_cosecha', 'ID CLIENTE':'identifi_cliente', 'CONFIRMACION ZONA':'confirmacion_zona',
                                       'OBSERVACION ASEGURADO':'obs_asegurado', 'OBSERVACION CEDULA ASEGURADO':'obs_ced_aseg', 'TIPO DE CREDITO':'tipo_credito',
                                       'FECHA DESEMBOLSO':'fec_desembolso', 'F.NACIMIENTO':'fec_nacimiento', 'OBSERVACION FECHA DE NACIMIENTO':'obs_fec_nacimiento',
                                       'FASECOLDA OK':'fasecolda_ok', 'REFERENCIA 1':'ref_1', 'REFERENCIA 3':'ref_3', 'VL ASEGURADO CONFIRMADO':'vl_aseg',
                                       'FECHA FIN ENDOSO CONFIRMADA':'fec_fin_endoso', 'ASEGURADORA ACTUAL':'aseg_actual', 'INSPECCION POR VIGENCIA Y/O SINIESTRO':'inspeccion',
                                       'TIPO DE POLIZA':'tipo_poliza'}                                      
                                      )
    return data

  
def cargar_reporte_bd(data):
    """Funcion que realiza la carga del reporte limpio a la BD para posterior dectura desde PBI"""
    cursor = cnxn.cursor()
    cursor.execute("truncate table [dbo].[seg_report_cosecha]")
    for index, row in data.iterrows():
        cursor.execute("INSERT INTO [Seguros_BdN].[dbo].[seg_report_cosecha] values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", 
                    row.COSECHA, row.anio_cosecha, row.mes_cosecha, row.OBLIGACION, row.identifi_cliente, row.CLIENTE, row.CONCESIONARIO, row.CIUDAD, row.confirmacion_zona, row.obs_asegurado,
                   row.obs_ced_aseg, row.OCUPACION, row.GENERO, row.tipo_credito, row.fec_desembolso, row.fec_nacimiento, row.obs_fec_nacimiento, row.PLACA, row.MODELO, row.FASECOLDA, row.fasecolda_ok,
                   row.SERIE, row.MARCA, row.CLASE, row.ref_1, row.ref_3, row.vl_aseg, row.SERVICIO, row.MEMO, row.fec_fin_endoso, row.aseg_actual, row.inspeccion, row.tipo_poliza, row.PLAN)
        cnxn.commit()
    cursor.close()
    

#Aseguradoras------------------
def extraer_aseguradoras(data):
    """Funcion que realiza la extraccion de DataFrame final de las aseguradoras para cargar a BD"""
    aseguradoras_1 = cosechas["ASEGURADORA ACTUAL"]
    aseguradoras_1 = pd.Series.to_frame(aseguradoras_1)
    aseguradoras_1 = aseguradoras_1.drop_duplicates(aseguradoras_1.columns[aseguradoras_1.columns.isin(['ASEGURADORA ACTUAL'])], keep='first')
    aseguradoras_1["ASEGURADORA ACTUAL"] = aseguradoras_1["ASEGURADORA ACTUAL"].str.capitalize()  
    aseguradoras_2 = pd.read_sql("SELECT [aseguradora] AS 'ASEGURADORA ACTUAL' FROM [Seguros_BdN].[dbo].[seg_aseguradoras] WITH(NOLOCK)", cnxn)
    aseguradoras_fn = pd.concat([aseguradoras_1, aseguradoras_2])
    aseguradoras_fn = aseguradoras_fn.drop_duplicates(aseguradoras_fn.columns[aseguradoras_fn.columns.isin(['ASEGURADORA ACTUAL'])], keep='first')
    aseguradoras_fn = aseguradoras_fn.rename(columns={'ASEGURADORA ACTUAL':'Aseguradora'})
    return aseguradoras_fn

def cargar_aseguradoras_bd(data):
    """Funcion que realiza la carga del listado de aseguradoras en la base de datos"""
    cursor = cnxn.cursor()
    cursor.execute("truncate table [dbo].[seg_aseguradoras]")
    for index, row in data.iterrows():
        cursor.execute("INSERT INTO [dbo].[seg_aseguradoras] values(?)", row.Aseguradora)
        cnxn.commit()
    cursor.close()
    

#Concesionarios------------------
def extraer_concesionarios(data):
    """Funcion que realiza la extraccion de DataFrame final de los concesionarios para cargar a BD"""
    concesionarios_1 = cosechas["CONCESIONARIO"]
    concesionarios_1 = pd.Series.to_frame(concesionarios_1)
    concesionarios_1 = concesionarios_1.drop_duplicates(concesionarios_1.columns[concesionarios_1.columns.isin(['CONCESIONARIO'])], keep='first')
    concesionarios_1["CONCESIONARIO"] = concesionarios_1["CONCESIONARIO"].str.capitalize()
    concesionarios_2 = pd.read_sql("SELECT [concesionario] AS 'CONCESIONARIO' FROM [Seguros_BdN].[dbo].[seg_concesionarios] WITH(NOLOCK)", cnxn)
    concesionarios_fn = pd.concat([concesionarios_1, concesionarios_2])
    concesionarios_fn = concesionarios_fn.drop_duplicates(concesionarios_fn.columns[concesionarios_fn.columns.isin(['CONCESIONARIO'])], keep='first')
    concesionarios_fn = concesionarios_fn.rename(columns={'CONCESIONARIO':'Concesionario'})
    return concesionarios_fn
    
def cargar_concesionarios_bd(data):
    """Funcion que realiza la carga del listado de concesionarios en la base de datos"""
    cursor = cnxn.cursor()
    cursor.execute("truncate table [dbo].[seg_concesionarios]")
    for index, row in data.iterrows():
        cursor.execute("INSERT INTO [dbo].[seg_concesionarios] values(?)", row.Concesionario)
        cnxn.commit()
    cursor.close()
    
    
#Clase Vehiculo-------------
def extraer_clase_veh(data):
    """Funcion que realiza la extraccion de DataFrame final de las clases de vehiculos para cargar a BD"""
    clasevehi_1 = cosechas["CLASE"]
    clasevehi_1 = pd.Series.to_frame(clasevehi_1)
    clasevehi_1 = clasevehi_1.drop_duplicates(clasevehi_1.columns[clasevehi_1.columns.isin(['CLASE'])], keep='first')
    clasevehi_1["CLASE"] = clasevehi_1["CLASE"].str.capitalize()
    clasevehi_2 = pd.read_sql("SELECT [clase_vehiculo] AS 'CLASE' FROM [Seguros_BdN].[dbo].[seg_clase_veh] WITH(NOLOCK)", cnxn)
    clasevehi_fn = pd.concat([clasevehi_1, clasevehi_2])
    clasevehi_fn = clasevehi_fn.drop_duplicates(clasevehi_fn.columns[clasevehi_fn.columns.isin(['CLASE'])], keep='first')
    clasevehi_fn = clasevehi_fn.rename(columns={'CLASE':'Clase'})
    return clasevehi_fn

def cargar_clases_veh_bd(data):
    """Funcion que realiza la carga del listado de clases de vehiculos en la base de datos"""
    cursor = cnxn.cursor()
    cursor.execute("truncate table [dbo].[seg_clase_veh]")
    for index, row in data.iterrows():
        cursor.execute("INSERT INTO [dbo].[seg_clase_veh] values(?)", row.Clase)
        cnxn.commit()
    cursor.close()
    
    
#Vehiculos-----------------
def extraer_vehiculos(data):
    """Funcion que realiza la extraccion de DataFrame final de los vehiculos para cargar a BD"""
    vehiculos_1 = cosechas.loc[:,["MARCA","CLASE","REFERENCIA 1","REFERENCIA 3", "VL ASEGURADO CONFIRMADO"]]
    #vehiculos_1 = pd.Series.to_frame(vehiculos_1)
    vehiculos_1 = vehiculos_1.drop_duplicates(vehiculos_1.columns[vehiculos_1.columns.isin(['MARCA','CLASE','REFERENCIA 1','REFERENCIA 3','VL ASEGURADO CONFIRMADO'])], keep='first')
    vehiculos_1["MARCA"] = vehiculos_1["MARCA"].str.capitalize()
    vehiculos_1["CLASE"] = vehiculos_1["CLASE"].str.capitalize()
    vehiculos_1["REFERENCIA 1"] = vehiculos_1["REFERENCIA 1"].str.capitalize()
    script = "SELECT [marca] AS 'MARCA', [clase_vehiculo] AS 'CLASE', [referencia_1] AS 'REFERENCIA 1', [referencia_3] AS 'REFERENCIA 3', [valor_asegurado] AS 'VL ASEGURADO CONFIRMADO' FROM [Seguros_BdN].[dbo].[seg_vehiculos] WITH(NOLOCK)"
    vehiculos_2 = pd.read_sql(script, cnxn)
    vehiculos_fn = pd.concat([vehiculos_1, vehiculos_2])
    vehiculos_fn = vehiculos_fn.drop_duplicates(vehiculos_fn.columns[vehiculos_fn.columns.isin(['MARCA','CLASE','REFERENCIA 1','REFERENCIA 3','VL ASEGURADO CONFIRMADO'])], keep='first')
    vehiculos_fn = vehiculos_fn.rename(columns={'MARCA':'Marca','CLASE':'Clase','REFERENCIA 1':'Referencia_1','REFERENCIA 3':'Referencia_3','VL ASEGURADO CONFIRMADO':'Vl_Asegurado',})
    return vehiculos_fn

def cargar_vehiculos_bd(data):
    """Funcion que realiza la carga del listado de vehiculos en la base de datos"""
    cursor = cnxn.cursor()
    cursor.execute("truncate table [dbo].[seg_vehiculos]")
    for index, row in data.iterrows():
        cursor.execute("INSERT INTO [dbo].[seg_vehiculos] values(?,?,?,?,?)", row.Marca, row.Clase, row.Referencia_1, row.Referencia_3, row.Vl_Asegurado)
        cnxn.commit()
    cursor.close()
    
 
#Servicio Vehiculo--------------
def extraer_serv_vehiculo(data):
    """Funcion que realiza la extraccion de DataFrame final de los servicios del vehiculo para cargar a BD"""
    serv_veh_1 = cosechas["SERVICIO"]
    serv_veh_1 = pd.Series.to_frame(serv_veh_1)
    serv_veh_1 = serv_veh_1.drop_duplicates(serv_veh_1.columns[serv_veh_1.columns.isin(['SERVICIO'])], keep='first')
    serv_veh_1["SERVICIO"] = serv_veh_1["SERVICIO"].str.capitalize()
    serv_veh_2 = pd.read_sql("SELECT [servicio_vehiculo] AS 'SERVICIO' FROM [Seguros_BdN].[dbo].[seg_servicio_veh] WITH(NOLOCK)", cnxn)
    serv_veh_fn = pd.concat([serv_veh_1, serv_veh_2])
    serv_veh_fn = serv_veh_fn.drop_duplicates(serv_veh_fn.columns[serv_veh_fn.columns.isin(['SERVICIO'])], keep='first')
    serv_veh_fn = serv_veh_fn.rename(columns={'SERVICIO':'Servicio'})
    return serv_veh_fn

def cargar_serv_vehiculos_bd(data):
    """Funcion que realiza la carga del listado de los servicios del vehiculo en la base de datos"""
    cursor = cnxn.cursor()
    cursor.execute("truncate table [dbo].[seg_servicio_veh]")
    for index, row in data.iterrows():
        cursor.execute("INSERT INTO [dbo].[seg_servicio_veh] values(?)", row.Servicio)
        cnxn.commit()
    cursor.close()
    
    
#Ciudades------------------
def extraer_ciudades(data):
    """Funcion que realiza la extraccion del DataFrame final de las ciudades para cargar a BD"""
    ciudades_1 = cosechas["CONFIRMACION ZONA"]
    ciudades_1 = pd.Series.to_frame(ciudades_1)
    ciudades_1 = ciudades_1.drop_duplicates(ciudades_1.columns[ciudades_1.columns.isin(['CONFIRMACION ZONA'])], keep='first')
    ciudades_1["CONFIRMACION ZONA"] = ciudades_1["CONFIRMACION ZONA"].str.capitalize()
    ciudades_2 = pd.read_sql("SELECT [ciudad] as 'CONFIRMACION ZONA' FROM [Seguros_BdN].[dbo].[seg_ciudades] WITH(NOLOCK)", cnxn)
    ciudades_fn = pd.concat([ciudades_1, ciudades_2])
    ciudades_fn = ciudades_fn.drop_duplicates(ciudades_fn.columns[ciudades_fn.columns.isin(['CONFIRMACION ZONA'])], keep='first')
    ciudades_fn = ciudades_fn.rename(columns={'CONFIRMACION ZONA':'Ciudad'})
    return ciudades_fn

def cargar_ciudades_bd(data):
    """Funcion que realiza la carga del listado de ciudades en la base de datos"""
    cursor = cnxn.cursor()
    cursor.execute("truncate table [dbo].[seg_ciudades]")
    for index, row in data.iterrows():
        cursor.execute("INSERT INTO [dbo].[seg_ciudades] values(?)", row.Ciudad)
        cnxn.commit()
    cursor.close()
    
 
#Clientes------------------
def extraer_clientes(data):
    """Funcion que realiza la extraccion del DataFrame final de los clientes para cargar a BD"""
    clientes_1 = cosechas.loc[:,["ID CLIENTE","CLIENTE"]]
    #vehiculos_1 = pd.Series.to_frame(vehiculos_1)
    clientes_1 = clientes_1.drop_duplicates(clientes_1.columns[clientes_1.columns.isin(['ID CLIENTE','CLIENTE'])], keep='first')
    clientes_1["CLIENTE"] = clientes_1["CLIENTE"].str.upper()
    clientes_2 = pd.read_sql("SELECT [identificacion_cli] as 'ID CLIENTE', upper([cliente]) as 'CLIENTE' FROM [Seguros_BdN].[dbo].[seg_clientes] WITH(NOLOCK)", cnxn)
    clientes_fn = pd.concat([clientes_1, clientes_2])
    clientes_fn = clientes_fn.drop_duplicates(clientes_fn.columns[clientes_fn.columns.isin(['ID CLIENTE','CLIENTE'])], keep='first')
    clientes_fn = clientes_fn.rename(columns={'ID CLIENTE':'id_cliente', 'CLIENTE':'Cliente'})
    return clientes_fn

def cargar_clientes_bd(data):
    """Funcion que realiza la carga del listado de ciudades en la base de datos"""
    cursor = cnxn.cursor()
    cursor.execute("truncate table [dbo].[seg_clientes]")
    for index, row in data.iterrows():
        cursor.execute("INSERT INTO [dbo].[seg_clientes] values(?,?)", row.id_cliente, row.Cliente)
        cnxn.commit()
    cursor.close()
    
#Cosechas------------------
def extraer_cosechas(data):
    """Funcion que realiza la extraccion del DataFrame final de las cosechas para cargar a BD"""
    bases_1 = cosechas["COSECHA"]
    bases_1 = pd.Series.to_frame(bases_1)
    bases_1 = bases_1.drop_duplicates(bases_1.columns[bases_1.columns.isin(['COSECHA'])], keep='first')
    bases_1["COSECHA"] = bases_1["COSECHA"].str.capitalize()
    bases_2 = pd.read_sql("SELECT [cosecha] AS 'COSECHA' FROM [Seguros_BdN].[dbo].[seg_tipo_cosechas] WITH(NOLOCK)", cnxn)
    bases_fn = pd.concat([bases_1, bases_2])
    bases_fn = bases_fn.drop_duplicates(bases_fn.columns[bases_fn.columns.isin(['COSECHA'])], keep='first')
    bases_fn = bases_fn.rename(columns={'COSECHA':'Cosecha'})
    return bases_fn

def cargar_cosechas_bd(data):
    """Funcion que realiza la carga del listado de tipo de cosechas en la base de datos"""
    cursor = cnxn.cursor()
    cursor.execute("truncate table [dbo].[seg_tipo_cosechas]")
    for index, row in data.iterrows():
        cursor.execute("INSERT INTO [dbo].[seg_tipo_cosechas] values(?)", row.Cosecha)
        cnxn.commit()
    cursor.close()