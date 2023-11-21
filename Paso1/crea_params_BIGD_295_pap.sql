---PARAMETROS PARA LA ENTIDAD RPRTPDDSCNLS0010
DELETE FROM params WHERE entidad='RPRTPDDSCNLS0010';
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('RPRTPDDSCNLS0010','PARAM1_FECHA_EJEC','date_format(sysdate(),''%Y%m%d'')','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('RPRTPDDSCNLS0010','ETAPA','1','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('RPRTPDDSCNLS0010','SHELL','/RGenerator/reportes/PEDIDOS_CANALES/bin/REPORTE_PEDIDOS_CANALES.sh','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('RPRTPDDSCNLS0010','VAL_RUTA','/RGenerator/reportes/PEDIDOS_CANALES','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('RPRTPDDSCNLS0010','VAL_RUTA_OUT','/RGenerator/reportes/PEDIDOS_CANALES/output/ReporteLogistica.csv','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('RPRTPDDSCNLS0010','EVENTO','1','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('RPRTPDDSCNLS0010','VAL_HORA_INI_1','13:20:01','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('RPRTPDDSCNLS0010','VAL_HORA_FIN_1','10:20:00','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('RPRTPDDSCNLS0010','VAL_HORA_INI_2','10:20:01','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('RPRTPDDSCNLS0010','VAL_HORA_FIN_2','11:50:00','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('RPRTPDDSCNLS0010','VAL_HORA_INI_3','11:50:01','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('RPRTPDDSCNLS0010','VAL_HORA_FIN_3','13:20:00','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('RPRTPDDSCNLS0010','QUEUE','capa_semantica','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('RPRTPDDSCNLS0010','VAL_TABLA_FINAL','db_reportes.otc_t_rep_pedidos_canales','0','1'); 
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('RPRTPDDSCNLS0010','VAL_MASTER','local','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('RPRTPDDSCNLS0010','VAL_DRIVER_MEMORY','32G','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('RPRTPDDSCNLS0010','VAL_EXECUTOR_MEMORY','32G','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('RPRTPDDSCNLS0010','VAL_NUM_EXECUTORS','8','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('RPRTPDDSCNLS0010','VAL_NUM_EXECUTOR_CORES','8','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('RPRTPDDSCNLS0010','VAL_CORREO_ASUNTO','Reporte pedidos canales','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('RPRTPDDSCNLS0010','VAL_CORREO_EMISOR','cortiz@softconsulting.com.ec','0','1');
INSERT INTO params(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('RPRTPDDSCNLS0010','VAL_CORREOS_RECEPTORES','Ingreso.telefonica@itsanet.com,despacho.telefonica@itsanet.com,logistica.ec@telefonica.com','0','1');
SELECT * FROM params WHERE ENTIDAD='RPRTPDDSCNLS0010';