								----- SCRIPT PARA VALIDACION CANTIDAD DE REGISTROS POR TABLA -----

--Aseguradoras
SELECT COUNT(1) AS CANTIDAD_ASEGURADORAS FROM seg_aseguradoras

--Ciudades
SELECT COUNT(1) AS CANTIDAD_CCIUDADES FROM seg_ciudades

--Clase de vehiculos
SELECT COUNT(1) AS CANTIDAD_CLASE_VEHICULOS FROM seg_clase_veh

--Clientes
SELECT COUNT(1) AS CANTIDAD_CLIENTES FROM seg_clientes

--Concesionarios
SELECT COUNT(1) AS CANTIDAD_CONCESIONARIOS FROM seg_concesionarios

--Servicios de Vehiculo
SELECT COUNT(1) AS CANTIDAD_VEHICULOS FROM seg_servicio_veh

--Tipo Bases
SELECT COUNT(1) AS CANTIDAD_BASES FROM seg_tipo_cosechas

--Vehiculos
SELECT COUNT(1) AS CANTIDAD_VEHICULOS FROM seg_vehiculos

--Reporte General
SELECT COUNT(1) AS CANTIDAD_CREDITOS FROM seg_report_cosecha