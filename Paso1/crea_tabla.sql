CREATE EXTERNAL TABLE db_reportes.otc_t_rep_pedidos_canales
(
  `work_item_id` string,
  `created_when` string,
  `type_` string,
  `mo_type` string,
  `movement_order_id` string,
  `mo_status` string,
  `customer_id` string,
  `customer_name` string,
  `contextid` string,
  `so_type` string,
  `equipment_condition` string,
  `preactivation_template_id` string,
  `preactivation_template_name` string,
  `equipment_name` string,
  `equipment_id` decimal(38,0),
  `article` string,
  `move_order_line_id` string,
  `location_from_name` string,
  `location_from_id` string,
  `location_to_name` string,
  `location_to_id` string,
  `approved_quantity` string,
  `reserved_quantity` int,
  `dispatched_quantity` int,
  `received_quantity` int,
  `to_be_dispatched` int,
  `fecha_ejecucion` bigint,
  `corte` string
)
COMMENT 'Tabla historica con la informacion del REPORTE PEDIDOS CANALES'
STORED AS PARQUET
TBLPROPERTIES('transactional'='false'
,'external.table.purge'='true',
'PARQUET.COMPRESS'='SNAPPY');

