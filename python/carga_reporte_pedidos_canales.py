import sys
reload(sys)
#sys.setdefaultencoding('windows-1252')
from query import *
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import *
import argparse
sys.path.insert(1, '/var/opt/tel_spark')
from messages import *
from functions import *
from create import *

## STEP 1: Definir variables o constantes
vLogInfo='INFO:'
vLogError='ERROR:'

timestart = datetime.now()
## STEP 2: Captura de argumentos en la entrada
parser = argparse.ArgumentParser()
parser.add_argument('--vTFinal', required=True, type=str,help='Parametro tabla final HIVE')
parser.add_argument('--vROut', required=True, type=str,help='Parametro ruta de salida archivo CSV')
parser.add_argument('--vDiaIni', required=True, type=str,help='Parametro dia inicial')
parser.add_argument('--vDiaFin', required=True, type=str,help='Parametro dia inicial')
parser.add_argument('--vHoraIni', required=True, type=str,help='Parametro dia inicial')
parser.add_argument('--vHoraFin', required=True, type=str,help='Parametro dia inicial')
parser.add_argument('--vJdbcUrl', required=True, type=str,help='Parametro dia inicial')
parser.add_argument('--vTDPass', required=True, type=str,help='Nombre de tabla de salida ')
parser.add_argument('--vTDUser', required=True, type=str,help='Nombre de tabla de salida ')
parser.add_argument('--vTDClass', required=True, type=str,help='Nombre de tabla de salida ')

parametros = parser.parse_args()
vTFinal=parametros.vTFinal
vROut=parametros.vROut
vDiaIni=parametros.vDiaIni
vDiaFin=parametros.vDiaFin
vHoraIni=parametros.vHoraIni
vHoraFin=parametros.vHoraFin
vJdbcUrl=parametros.vJdbcUrl
vTDPass=parametros.vTDPass
vTDUser=parametros.vTDUser
vTDClass=parametros.vTDClass

## STEP 3: Inicio el SparkSession
spark = SparkSession. \
    builder. \
    config("hive.exec.dynamic.partition.mode", "nonstrict"). \
    enableHiveSupport(). \
    getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
app_id = spark._sc.applicationId
print(etq_info("INFO: Mostrar application_id => {}".format(str(app_id))))

##STEP 4:QUERYS
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
			.option("fetchsize",1000)\
			.option("dbtable",vSQL).load()
    df01.printSchema()
    ts_step_tbl = datetime.now()
    #df01.repartition(1).write.mode("overwrite").format('parquet').saveAsTable(vTFinal)
    print(etq_info('Spark datafrmae a pandas DF:'))
    pandas_df = df01.toPandas()
    print(etq_info('Generando CSV...:'))
    #pandas_df.to_csv(vROut, sep=';',index=False, chunksize=10)
    pandas_df.to_csv(vROut, sep=';',index=False, encoding='windows-1252')
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
