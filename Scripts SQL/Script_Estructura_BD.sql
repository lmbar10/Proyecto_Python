													--- CREACION BASE DE DATOS SEGUROS BANCO COMERCIAL ---
													------------------------------------------------------
													
--1. Crear tabla de aseguradoras
CREATE TABLE seg_aseguradoras(
id_aseguradora int identity primary key clustered,
aseguradora varchar(30))

SELECT * FROM seg_aseguradoras

--2. Crear tabla de concesionarios
CREATE TABLE seg_concesionarios(
id_concesionario int identity primary key clustered,
concesionario varchar(100))

SELECT * FROM seg_concesionarios

--3. Crear tabla de Clases de vehiculo
CREATE TABLE seg_clase_veh(
id_clase_veh int identity primary key clustered,
clase_vehiculo varchar(60))

SELECT * FROM seg_clase_veh

--4. Crear tabla de Vehiculos
CREATE TABLE seg_vehiculos(
id_vehiculo int identity primary key clustered,
marca varchar(50),
clase_vehiculo varchar(60),
referencia_1 varchar(50),
referencia_3 varchar(50),
valor_asegurado money)

SELECT * FROM seg_vehiculos

--5. Crear tabla de Servicios de Vehiculos
CREATE TABLE seg_servicio_veh(
id_servicio_veh int identity primary key clustered,
servicio_vehiculo varchar(60))

SELECT * FROM seg_servicio_veh

--6. Crear tabla de Ciudades
CREATE TABLE seg_ciudades(
id_ciudad int identity primary key clustered,
ciudad varchar(100))

SELECT * FROM seg_ciudades

--7. Crear tabla de Clientes
CREATE TABLE seg_clientes(
id_cliente int identity primary key clustered,
identificacion_cli int,
cliente varchar(200))

SELECT * FROM seg_clientes

--8. Crear Tabla Tipo Cosechas
CREATE TABLE seg_tipo_cosechas(
id_cosecha int identity primary key clustered,
cosecha varchar(50))

SELECT * FROM seg_tipo_cosechas


--------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------
--1. Crear Tabla Report Cosecha
CREATE TABLE seg_report_cosecha(
id_reg int identity primary key clustered,
cosecha varchar(50),
anio_cosecha varchar(5),
mes_cosecha varchar(5),
obligacion varchar(50),
identificacion_cli int,
cliente varchar(100),
concesionario varchar(100),
ciudad varchar(100),
confirmacion_zona varchar(100),
obser_asegurado varchar(100),
obser_ced_asegurado varchar(50),
ocupacion varchar(100),
genero varchar(50),
tipo_credito varchar(100),
fecha_desembolso varchar(50),
fec_nacimiento varchar(50),
obser_f_nacimiento varchar(50),
placa varchar(50),
modelo varchar(5),
fasecolda varchar(50),
fasecolda_ok varchar(50),
serie varchar(50),
marca varchar(50),
clase varchar(50),
referencia_1 varchar(100),
referencia_3 varchar(100),
vr_asegurado money,
servicio varchar(50),
memo varchar(100),
fec_fin_endoso varchar(50),
aseguradora_actual varchar(50),
inspeccion varchar(50),
tipo_poliza varchar(50),
plan_cre varchar(50))
