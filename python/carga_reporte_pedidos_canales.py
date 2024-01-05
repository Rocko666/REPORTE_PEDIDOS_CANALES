import sys
reload(sys)
#sys.setdefaultencoding('windows-1252')
from query import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType
from datetime import datetime
from pyspark.sql.functions import *
import argparse
sys.path.insert(1, '/var/opt/tel_spark')
from messages import *
from functions import *
from create import *


timestart = datetime.now()
## STEP 1: Captura de argumentos en la entrada
parser = argparse.ArgumentParser()
parser.add_argument('--vTFinal', required=True, type=str,help='Parametro tabla final HIVE')
parser.add_argument('--vTTmp', required=True, type=str,help='Parametro tabla temporal HIVE')
parser.add_argument('--vROut', required=True, type=str,help='Parametro ruta de salida archivo CSV')
parser.add_argument('--vDiaIni', required=True, type=str,help='Parametro dia inicial')
parser.add_argument('--vDiaFin', required=True, type=str,help='Parametro dia final')
parser.add_argument('--vHoraIni', required=True, type=str,help='Parametro hora inicial')
parser.add_argument('--vHoraFin', required=True, type=str,help='Parametro hora final')
parser.add_argument('--vJdbcUrl', required=True, type=str,help='Parametro JDBC')
parser.add_argument('--vTDPass', required=True, type=str,help='Parametro pass JDBC')
parser.add_argument('--vTDUser', required=True, type=str,help='Parametro user JDBC')
parser.add_argument('--vFProc', required=True, type=str,help='Parametro de fecha proceso ')
parser.add_argument('--vTCarga', required=True, type=str,help='Parametro de tipo de carga ')
parser.add_argument('--vRepartition', required=True, type=int,help='Parametro del repartition ')
parser.add_argument('--vFetchSize', required=True, type=str,help='Parametro del fetch size ')
parser.add_argument('--vTDClass', required=True, type=str,help='Parametro TDClass')

parametros = parser.parse_args()
vTFinal=parametros.vTFinal
vTTmp=parametros.vTTmp
vROut=parametros.vROut
vDiaIni=parametros.vDiaIni
vDiaFin=parametros.vDiaFin
vHoraIni=parametros.vHoraIni
vHoraFin=parametros.vHoraFin
vJdbcUrl=parametros.vJdbcUrl
vTDPass=parametros.vTDPass
vTDUser=parametros.vTDUser
vFProc=parametros.vFProc
vTCarga=parametros.vTCarga
vRepartition=parametros.vRepartition
vFetchSize=parametros.vFetchSize
vTDClass=parametros.vTDClass

print(etq_info("Imprimiendo parametros para SPARK:"))
print(lne_dvs())
print(etq_info(log_p_parametros("Tabla final HIVE",str(vTFinal))))
print(etq_info(log_p_parametros("Tabla temporal HIVE",str(vTTmp))))
print(etq_info(log_p_parametros("Ruta de salida archivo CSV",str(vROut))))
print(etq_info(log_p_parametros("Dia inicial",str(vDiaIni))))
print(etq_info(log_p_parametros("Dia final",str(vDiaFin))))
print(etq_info(log_p_parametros("Hora inicial",str(vHoraIni))))
print(etq_info(log_p_parametros("Hora final",str(vHoraFin)))) 
print(etq_info(log_p_parametros("URL JDBC",str(vJdbcUrl)))) 
print(etq_info(log_p_parametros("User JDBC",str(vTDUser)))) 
print(etq_info(log_p_parametros("Fecha proceso",str(vFProc)))) 
print(etq_info(log_p_parametros("Tipo de carga de tablas HIVE",str(vTCarga)))) 
print(etq_info(log_p_parametros("Parametro del repartition",str(vRepartition))))
print(etq_info(log_p_parametros("Parametro del fetch size",str(vFetchSize))))
print(etq_info(log_p_parametros("Parametro TDClass",str(vTDClass))))
print(lne_dvs())
    
## STEP 2: Inicio del SparkSession
spark = SparkSession. \
    builder. \
    config("hive.exec.dynamic.partition.mode", "nonstrict"). \
    enableHiveSupport(). \
    getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
app_id = spark._sc.applicationId
print(etq_info("INFO: Mostrar application_id => {}".format(str(app_id))))

##STEP 3:QUERYS
print(lne_dvs())
timestart_b = datetime.now()
try:
    
    vStp="Paso [01]: Ejecucion de funcion [otc_t_xtrc_oracle]- Creacion de Dataframe con lectura desde ORACLE"
    print(lne_dvs())
    print(etq_info(vStp))
    print(lne_dvs())
    vSQL=otc_t_xtrc_oracle(vDiaIni,vDiaFin,vHoraIni,vHoraFin)
    print(etq_sql(vSQL))
    df01 = spark.read.format("jdbc")\
			.option("url",vJdbcUrl)\
			.option("driver",vTDClass)\
			.option("user",vTDUser)\
			.option("password",vTDPass)\
			.option("fetchsize",vFetchSize)\
			.option("dbtable","({})".format(vSQL))\
            .load()
    df01=df01.cache()
    print(etq_info("Culmina lectura"))
    
    # Aplicar el esquema a las columnas correspondientes
    df01 = df01.withColumn("WORK_ITEM_ID", col("WORK_ITEM_ID").cast(StringType())) \
            .withColumn("CREATED_WHEN", col("CREATED_WHEN").cast(StringType())) \
            .withColumn("TYPE_", col("TYPE_").cast(StringType())) \
            .withColumn("MO_TYPE", col("MO_TYPE").cast(StringType())) \
            .withColumn("MOVEMENT_ORDER_ID", col("MOVEMENT_ORDER_ID").cast(StringType())) \
            .withColumn("MO_STATUS", col("MO_STATUS").cast(StringType())) \
            .withColumn("CUSTOMER_ID", col("CUSTOMER_ID").cast(StringType())) \
            .withColumn("CUSTOMER_NAME", col("CUSTOMER_NAME").cast(StringType())) \
            .withColumn("CONTEXTID", col("CONTEXTID").cast(StringType())) \
            .withColumn("SO_TYPE", col("SO_TYPE").cast(StringType())) \
            .withColumn("EQUIPMENT_CONDITION", col("EQUIPMENT_CONDITION").cast(StringType())) \
            .withColumn("PREACTIVATION_TEMPLATE_ID", col("PREACTIVATION_TEMPLATE_ID").cast(StringType())) \
            .withColumn("PREACTIVATION_TEMPLATE_NAME", col("PREACTIVATION_TEMPLATE_NAME").cast(StringType())) \
            .withColumn("EQUIPMENT_NAME", col("EQUIPMENT_NAME").cast(StringType())) \
            .withColumn("EQUIPMENT_ID", col("EQUIPMENT_ID").cast(DecimalType(38,0))) \
            .withColumn("ARTICLE", col("ARTICLE").cast(StringType())) \
            .withColumn("MOVE_ORDER_LINE_ID", col("MOVE_ORDER_LINE_ID").cast(StringType())) \
            .withColumn("LOCATION_FROM_NAME", col("LOCATION_FROM_NAME").cast(StringType())) \
            .withColumn("LOCATION_FROM_ID", col("LOCATION_FROM_ID").cast(StringType())) \
            .withColumn("LOCATION_TO_NAME", col("LOCATION_TO_NAME").cast(StringType())) \
            .withColumn("LOCATION_TO_ID", col("LOCATION_TO_ID").cast(StringType())) \
            .withColumn("APPROVED_QUANTITY", col("APPROVED_QUANTITY").cast(StringType())) \
            .withColumn("RESERVED_QUANTITY", col("RESERVED_QUANTITY").cast(IntegerType())) \
            .withColumn("DISPATCHED_QUANTITY", col("DISPATCHED_QUANTITY").cast(IntegerType())) \
            .withColumn("RECEIVED_QUANTITY", col("RECEIVED_QUANTITY").cast(IntegerType())) \
            .withColumn("TO_BE_DISPATCHED", col("TO_BE_DISPATCHED").cast(IntegerType())) 
    #Conversion de DF a archivo .csv
    ts_step_tbl = datetime.now()
    print(etq_info('Spark dataframe a pandas DF:'))
    pandas_df = df01.toPandas()
    print(etq_info('Generando CSV...:'))
    pandas_df.to_csv(vROut, sep=';',index=False, encoding='windows-1252')
    #Aniadir columnas para filtrado de tabla historica 
    print(etq_info("Aniandiendo columnas: "))
    df01 = df01.withColumn("fecha_ejecucion", lit(vFProc))
    df01 = df01.withColumn("corte", lit(vHoraFin))
    df01.printSchema()
    #Eliminacion de informacion preexistente en caso de reproceso
    print(etq_info("Se elimina informacion preexistente de la tabla: {}".format(vTFinal)))
    df_filtrado = spark.sql(dlt_otc_t_rep_pedidos_canales(vTFinal,vDiaFin,vHoraFin))
    #Escritura de tabla temporal  
    df_filtrado.repartition(vRepartition).write.format("parquet").mode("overwrite").saveAsTable(vTTmp)
    del df_filtrado
    print(etq_info('Union de dataframes:'))
    df_final=spark.sql(otc_t_rep_pedidos_cnls_tmp(vTTmp))
    df_final= df_final.union(df01)
    print(etq_info("Inicia insert en la tabla: {}".format(vTFinal)))
    #Escritura de tabla historica
    df_final.repartition(vRepartition).write.mode(vTCarga).saveAsTable(vTFinal)
    del df_final
    print(etq_info("Culmina insert"))
    print(etq_info('CSV Generado exitosamente:'))
    
    print(etq_info(msg_t_total_registros_obtenidos("df01",str(df01.count())))) 
    te_step_tbl = datetime.now()
    print(etq_info(msg_d_duracion_hive("df01",vle_duracion(ts_step_tbl,te_step_tbl))))
    del df01
    
except Exception as e:
	exit(etq_error(msg_e_ejecucion(vStp,str(e))))

spark.stop()
timeend = datetime.now()
print(etq_info(msg_d_duracion_ejecucion("carga_reporte_pedidos_canales",vle_duracion(timestart,timeend))))
print(lne_dvs())
