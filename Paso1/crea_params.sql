---PARAMETROS PARA LA ENTIDAD D_RPRTPDDSCNLS0010
DELETE FROM params_des WHERE entidad='D_RPRTPDDSCNLS0010';
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_RPRTPDDSCNLS0010','SHELL','/home/nae108834/RGenerator/reportes/PEDIDOS_CANALES/bin/REPORTE_PEDIDOS_CANALES.sh','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_RPRTPDDSCNLS0010','VAL_RUTA','/home/nae108834/RGenerator/reportes/PEDIDOS_CANALES','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_RPRTPDDSCNLS0010','VAL_RUTA_OUT','/home/nae108834/RGenerator/reportes/PEDIDOS_CANALES/output/ReporteLogistica.csv','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_RPRTPDDSCNLS0010','PARAM1_FECHA_EJEC','date_format(sysdate(),''%Y%m%d'')','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_RPRTPDDSCNLS0010','ETAPA','1','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_RPRTPDDSCNLS0010','EVENTO','1','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_RPRTPDDSCNLS0010','VAL_DIA_INI','27/10/2023','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_RPRTPDDSCNLS0010','VAL_DIA_FIN','27/10/2023','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_RPRTPDDSCNLS0010','VAL_HORA_INI','10:20:01','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_RPRTPDDSCNLS0010','VAL_HORA_FIN','11:50:00','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_RPRTPDDSCNLS0010','QUEUE','capa_semantica','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_RPRTPDDSCNLS0010','VAL_TABLA_FINAL','db_desarrollo2021.otc_t_rprt_pedidos_canales','0','0'); 
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_RPRTPDDSCNLS0010','VAL_MASTER','local','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_RPRTPDDSCNLS0010','VAL_DRIVER_MEMORY','16G','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_RPRTPDDSCNLS0010','VAL_EXECUTOR_MEMORY','16G','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_RPRTPDDSCNLS0010','VAL_NUM_EXECUTORS','8','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_RPRTPDDSCNLS0010','VAL_NUM_EXECUTOR_CORES','8','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_RPRTPDDSCNLS0010','TDDB','tomstby.otecel.com.ec','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_RPRTPDDSCNLS0010','TDHOST','proxfulldg1.otecel.com.ec','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_RPRTPDDSCNLS0010','TDPASS','TelfEcu2017','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_RPRTPDDSCNLS0010','TDUSER','rdb_reportes','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_RPRTPDDSCNLS0010','TDPORT','7594','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_RPRTPDDSCNLS0010','VAL_CORREO_ASUNTO','Reporte pedidos canales','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_RPRTPDDSCNLS0010','VAL_CORREO_EMISOR','cortiz@softconsulting.com.ec','0','0');
INSERT INTO params_des(ENTIDAD,PARAMETRO,VALOR,ORDEN,AMBIENTE) VALUES('D_RPRTPDDSCNLS0010','VAL_CORREOS_RECEPTORES','cortiz@softconsulting.com.ec,gvelez@softconsulting.com.ec,daysi.quilumbasiavichay@telefonica.com,liliana.calderoncarrion@telefonica.com','0','0');
SELECT * FROM params_des WHERE ENTIDAD='D_RPRTPDDSCNLS0010';

