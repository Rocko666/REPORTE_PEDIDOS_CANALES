set -e
#########################################################################################################
# NOMBRE: REPORTE_PEDIDOS_CANALES.sh  		      												                        
# DESCRIPCION:																							                                            
# Shell que carga los datos desde Oracle a Hive		                                                 											             
# AUTOR: Cristian Ortiz - Softconsulting             														                          
# FECHA CREACION: 2023-11-09																			                                      
# PARAMETROS DEL SHELL                            													                            
# $1: Parametro de Fecha Inicial del proceso a ejecutar  								        		                    						                	
#########################################################################################################
# MODIFICACIONES																						                                            
# FECHA  		AUTOR     		                    DESCRIPCION MOTIVO		
# xxxx-xx-xx	........ (Softconsulting)                                                                                                         
#########################################################################################################

##############
# VARIABLES #
##############
ENTIDAD=RPRTPDDSCNLS0010
AMBIENTE=1 # AMBIENTE (1=produccion, 0=desarrollo)

if [ $AMBIENTE -gt 0 ]; then
    TABLA=params
else
    TABLA=params_des
fi

###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Lectura de parametros iniciales"
###########################################################################################################################################################
VAL_KINIT=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_KINIT';"`
$VAL_KINIT

###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Inicializacion del LOG"
###########################################################################################################################################################
VAL_HORA=`date '+%Y%m%d%H%M%S'`
VAL_RUTA=`mysql -N <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_RUTA';"` 
VAL_LOG=$VAL_RUTA/log/$ENTIDAD"_"$VAL_HORA.log
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Iniciando registro en el log.." 2>&1 &>> $VAL_LOG

###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Parametros definidos en la tabla $TABLA" 2>&1 &>> $VAL_LOG
###########################################################################################################################################################
VAL_RUTA_OUT=`mysql -N <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_RUTA_OUT';"` 
VAL_HORA_INI_1=`mysql -N  <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_HORA_INI_1';"`
VAL_HORA_FIN_1=`mysql -N  <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_HORA_FIN_1';"`
VAL_HORA_INI_2=`mysql -N  <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_HORA_INI_2';"`
VAL_HORA_FIN_2=`mysql -N  <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_HORA_FIN_2';"`
VAL_HORA_INI_3=`mysql -N  <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_HORA_INI_3';"`
VAL_HORA_FIN_3=`mysql -N  <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_HORA_FIN_3';"`
VAL_TABLA_FINAL=`mysql -N  <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TABLA_FINAL';"`
VAL_TABLA_TMP=`mysql -N  <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TABLA_TMP';"`
VAL_TIPO_CARGA=`mysql -N  <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_TIPO_CARGA';"`
VAL_REPARTITION=`mysql -N  <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_REPARTITION';"`
VAL_FETCH_SIZE=`mysql -N  <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_FETCH_SIZE';"`
VAL_MASTER=`mysql -N  <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_MASTER';"`
VAL_DRIVER_MEMORY=`mysql -N  <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_DRIVER_MEMORY';"`
VAL_EXECUTOR_MEMORY=`mysql -N  <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_EXECUTOR_MEMORY';"`
VAL_NUM_EXECUTORS=`mysql -N  <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NUM_EXECUTORS';"`
VAL_NUM_EXECUTOR_CORES=`mysql -N  <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NUM_EXECUTOR_CORES';"`
VAL_QUEUE=`mysql -N  <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'QUEUE';"`
VAL_CORREO_ASUNTO=`mysql -N  <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_CORREO_ASUNTO';"`
VAL_CORREO_EMISOR=`mysql -N  <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_CORREO_EMISOR';"`
VAL_CORREOS_RECEPTORES=`mysql -N  <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_CORREOS_RECEPTORES';"`
ETAPA=`mysql -N  <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'ETAPA';"`
EVENTO=`mysql -N  <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'EVENTO';"`
SHELL=`mysql -N  <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'SHELL';"`


###################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Obtener y validar parametros de oracle definidos en la tabla $TABLA..." 2>&1 &>> $VAL_LOG
###################################################################################################################
TDDB=`mysql -N  <<<"select valor from params where entidad = 'SPARK_GENERICO'  AND parametro = 'TDSERVICE_RDB';"`
TDUSER=`mysql -N  <<<"select valor from params where entidad = 'SPARK_GENERICO'  AND parametro = 'TDUSER_RDB';"`
TDPASS=`mysql -N  <<<"select valor from params where entidad = 'SPARK_GENERICO'  AND parametro = 'TDPASS_RDB';"`
TDHOST=`mysql -N  <<<"select valor from params where entidad = 'SPARK_GENERICO'  AND parametro = 'TDHOST_RDB2';"`
TDPORT=`mysql -N  <<<"select valor from params where entidad = 'SPARK_GENERICO'  AND parametro = 'TDPORT_RDB';"`
TDCLASS=`mysql -N  <<<"select valor from params where entidad = 'SPARK_GENERICO'  AND parametro = 'TDCLASS_ORC';"`

###################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Obtener y validar parametros genericos SPARK..." 2>&1 &>> $VAL_LOG
###################################################################################################################
VAL_RUTA_SPARK=`mysql -N  <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_RUTA_SPARK';"`
VAL_RUTA_LIB=`mysql -N  <<<"select valor from params where entidad = 'SPARK_GENERICO'  AND parametro = 'VAL_RUTA_LIB';"`
VAL_LIB=`mysql -N  <<<"select valor from params where entidad = 'SPARK_GENERICO'  AND parametro = 'VAL_NOM_JAR_ORC_11';"`

###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Parametros calculados" 2>&1 &>> $VAL_LOG
###########################################################################################################################################################
JDBCURL1=jdbc:oracle:thin:@$TDHOST:$TDPORT/$TDDB

###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Definir parametros por consola o ControlM" 2>&1 &>> $VAL_LOG
###########################################################################################################################################################
VAL_FECHA_PROCESO=$1

###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Validacion de parametros iniciales, nulos y existencia de Rutas " 2>&1 &>> $VAL_LOG
###########################################################################################################################################################
if  [ -z "$ENTIDAD" ] || 
	[ -z "$VAL_LOG" ] || 
	[ -z "$VAL_RUTA" ] || 
	[ -z "$VAL_RUTA_OUT" ] || 
	[ -z "$VAL_HORA_INI_1" ] || 
	[ -z "$VAL_HORA_FIN_1" ] || 
	[ -z "$VAL_HORA_INI_2" ] || 
	[ -z "$VAL_HORA_FIN_2" ] || 
	[ -z "$VAL_HORA_INI_3" ] || 
	[ -z "$VAL_HORA_FIN_3" ] || 
	[ -z "$VAL_TABLA_FINAL" ] || 
	[ -z "$VAL_TABLA_TMP" ] ||
	[ -z "$VAL_TIPO_CARGA" ] || 
	[ -z "$VAL_REPARTITION" ] || 
	[ -z "$VAL_FETCH_SIZE" ] || 
	[ -z "$VAL_MASTER" ] || 
	[ -z "$VAL_DRIVER_MEMORY" ] || 
	[ -z "$VAL_EXECUTOR_MEMORY" ] || 
	[ -z "$VAL_NUM_EXECUTORS" ] ||
	[ -z "$VAL_NUM_EXECUTOR_CORES" ] || 
	[ -z "$VAL_QUEUE" ] ||
	[ -z "$VAL_CORREO_ASUNTO" ] ||
	[ -z "$VAL_CORREO_EMISOR" ] ||
	[ -z "$VAL_CORREOS_RECEPTORES" ] ||
	[ -z "$ETAPA" ] ||
	[ -z "$EVENTO" ] ||
	[ -z "$SHELL" ] ||
	[ -z "$TDDB" ] ||
	[ -z "$TDUSER" ] ||
	[ -z "$TDPASS" ] ||
	[ -z "$TDHOST" ] ||
	[ -z "$TDPORT" ] ||
    [ -z "$TDCLASS" ] ||
	[ -z "$VAL_RUTA_SPARK" ] ||
	[ -z "$VAL_RUTA_LIB" ] ||
	[ -z "$VAL_LIB" ] ||
	[ -z "$JDBCURL1" ] ; then
echo `date '+%Y-%m-%d %H:%M:%S'`" ERROR: $TIME [ERROR] $rc unos de los parametros esta vacio o es nulo" 2>&1 &>> $VAL_LOG
error=1
exit $error
fi

case $EVENTO in
	1)
    echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Se procesa el EVENTO: $EVENTO" 2>&1 &>> $VAL_LOG

	VAL_DIA_INI=`date '+%d/%m/%Y' -d "$VAL_FECHA_PROCESO-1 day"`
	VAL_DIA_FIN=`date '+%d/%m/%Y' -d "$VAL_FECHA_PROCESO"`
	VAL_HORA_INI=$VAL_HORA_INI_1
	VAL_HORA_FIN=$VAL_HORA_FIN_1
	VAL_SIG_EVENTO=2

    ;;
	2)
    echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Se procesa el EVENTO: $EVENTO" 2>&1 &>> $VAL_LOG
    
	VAL_DIA_INI=`date '+%d/%m/%Y' -d "$VAL_FECHA_PROCESO"`
	VAL_DIA_FIN=`date '+%d/%m/%Y' -d "$VAL_FECHA_PROCESO"`
	VAL_HORA_INI=$VAL_HORA_INI_2
	VAL_HORA_FIN=$VAL_HORA_FIN_2
	VAL_SIG_EVENTO=3

    ;;
	3)
    echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Se procesa el EVENTO: $EVENTO" 2>&1 &>> $VAL_LOG
    
	VAL_DIA_INI=`date '+%d/%m/%Y' -d "$VAL_FECHA_PROCESO"`
	VAL_DIA_FIN=`date '+%d/%m/%Y' -d "$VAL_FECHA_PROCESO"`
	VAL_HORA_INI=$VAL_HORA_INI_3
	VAL_HORA_FIN=$VAL_HORA_FIN_3
	VAL_SIG_EVENTO=1

    ;;
	*)
    echo `date '+%Y-%m-%d %H:%M:%S'`" ERROR: Evento no valido" 2>&1 &>> $VAL_LOG
    ;;
esac

if [ "$ETAPA" = "1" ]; then
###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: ETAPA 1: Oracle Import " 2>&1 &>> $VAL_LOG
###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: DIA_INICIAL => " $VAL_DIA_INI 2>&1 &>> $VAL_LOG
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: DIA_FINAL => " $VAL_DIA_FIN 2>&1 &>> $VAL_LOG
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: HORA_INICIAL => " $VAL_HORA_INI 2>&1 &>> $VAL_LOG
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: HORA_FINAL => " $VAL_HORA_FIN 2>&1 &>> $VAL_LOG
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: JDBCURL1 => " $JDBCURL1 2>&1 &>> $VAL_LOG

$VAL_RUTA_SPARK \
--conf spark.port.maxRetries=100 \
--name $ENTIDAD \
--master $VAL_MASTER \
--driver-memory $VAL_DRIVER_MEMORY \
--executor-memory $VAL_EXECUTOR_MEMORY \
--num-executors $VAL_NUM_EXECUTORS \
--executor-cores $VAL_NUM_EXECUTOR_CORES \
--jars $VAL_RUTA_LIB/$VAL_LIB \
--queue $VAL_QUEUE \
$VAL_RUTA/python/carga_reporte_pedidos_canales.py \
--vTFinal=$VAL_TABLA_FINAL \
--vTTmp=$VAL_TABLA_TMP \
--vROut=$VAL_RUTA_OUT \
--vDiaIni=$VAL_DIA_INI \
--vDiaFin=$VAL_DIA_FIN \
--vHoraIni=$VAL_HORA_INI \
--vHoraFin=$VAL_HORA_FIN \
--vJdbcUrl=$JDBCURL1 \
--vTDPass=$TDPASS \
--vTDUser=$TDUSER \
--vTCarga=$VAL_TIPO_CARGA \
--vRepartition=$VAL_REPARTITION \
--vFetchSize=$VAL_FETCH_SIZE \
--vFProc=$VAL_FECHA_PROCESO \
--vTDClass=$TDCLASS 2>&1 &>> $VAL_LOG

echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: ETAPA 1 --> La carga de informacion a ORACLE fue ejecutada de manera EXITOSA" 2>&1 &>> $VAL_LOG	
ETAPA=2
#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: $SHELL --> Se procesa la ETAPA 1 con EXITO " 2>&1 &>> $VAL_LOG
`mysql -N  <<<"update $TABLA set valor='2' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA';"`
fi

if [ "$ETAPA" = "2" ]; then
###########################################################################################################################################################
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: ETAPA 2: Envio de archivo CSV por MAIL " 2>&1 &>> $VAL_LOG
###########################################################################################################################################################

echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Direccion de correo emisor  => " $VAL_CORREO_EMISOR 2>&1 &>> $VAL_LOG
echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: Direcciones de correo receptores => " $VAL_CORREOS_RECEPTORES 2>&1 &>> $VAL_LOG

	echo "Reporte:
	$VAL_DIA_INI $VAL_HORA_INI"-"$VAL_DIA_FIN $VAL_HORA_FIN
	" | mailx -s "${VAL_CORREO_ASUNTO}" \
	-a $VAL_RUTA_OUT \
	-S from=$VAL_CORREO_EMISOR $VAL_CORREOS_RECEPTORES
	echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: El MAIL con el archivo CSV se envia correctamente" 2>&1 &>> $VAL_LOG

	#SE REALIZA EL SETEO DE LA ETAPA EN LA TABLA params
		`mysql -N  <<<"update $TABLA set valor='1' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA';"`
		`mysql -N  <<<"update $TABLA set valor=$VAL_SIG_EVENTO where ENTIDAD = '${ENTIDAD}' and parametro = 'EVENTO';"`
		echo `date '+%Y-%m-%d %H:%M:%S'`" INFO: El proceso REPORTE_PEDIDOS_CANALES finaliza correctamente " 2>&1 &>> $VAL_LOG
fi

