                                                    ### PROYECTO PROCESAMIENTO BASES RENOVACION BANCO DEL NORTE ###
                                                    ###############################################################
                                                
                                                                ###EJECUCION DE FUNCIONES PARA CARGAR BD### 

--------------------------------------------------------------------------------------------------------------------------------------------------------------

#Aseguradoras
"""Se procede a cargar la dimension Aseguradora en la BD"""
aseguradoras_fn = extraer_aseguradoras(cosechas)
cargar_aseguradoras_bd(aseguradoras_fn)

#Concesionarios
"""Se procede a cargar la dimension Concesionarios en la BD"""
concesionarios_fn = extraer_concesionarios(cosechas)
cargar_concesionarios_bd(concesionarios_fn)

#Clases de Vehiculos
"""Se procede a cargar la dimension Clases de Vehiculos en la BD"""
clasevehi_fn = extraer_clase_veh(cosechas)
cargar_clases_veh_bd(clasevehi_fn)

#Vehiculos
"""Se procede a cargar la dimension Vehiculos en la BD"""
vehiculos_fn = extraer_vehiculos(cosechas)
cargar_vehiculos_bd(vehiculos_fn)

#Servicio de Vehiculos
"""Se procede a cargar la dimension Servicios de Vehiculos en la BD"""
serv_veh_fn = extraer_serv_vehiculo(cosechas)
cargar_serv_vehiculos_bd(serv_veh_fn)

#Ciudades
"""Se procede a cargar la dimension Ciudades en la BD"""
ciudades_fn = extraer_ciudades(cosechas)
cargar_ciudades_bd(ciudades_fn)

#Clientes
"""Se procede a cargar la dimension Clientes en la BD"""
clientes_fn = extraer_clientes(cosechas)
cargar_clientes_bd(clientes_fn)

#Cosechas
"""Se procede a cargar la dimension Cosechas en la BD"""
bases_fn = extraer_cosechas(cosechas)
cargar_cosechas_bd(bases_fn)

