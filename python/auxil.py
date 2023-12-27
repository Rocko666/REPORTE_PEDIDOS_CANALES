
    # Definir el esquema
    custom_schema = StructType([
        StructField("WORK_ITEM_ID", StringType(), True),
        StructField("CREATED_WHEN", StringType(), True),
        StructField("TYPE_", StringType(), True),
        StructField("MO_TYPE", StringType(), True),
        StructField("MOVEMENT_ORDER_ID", StringType(), True),
        StructField("MO_STATUS", StringType(), True),
        StructField("CUSTOMER_ID", StringType(), True),
        StructField("CUSTOMER_NAME", StringType(), True),
        StructField("CONTEXTID", StringType(), True),
        StructField("SO_TYPE", StringType(), True),
        StructField("EQUIPMENT_CONDITION", StringType(), True),
        StructField("PREACTIVATION_TEMPLATE_ID", StringType(), True),
        StructField("PREACTIVATION_TEMPLATE_NAME", StringType(), True),
        StructField("EQUIPMENT_NAME", StringType(), True),
        StructField("EQUIPMENT_ID", StringType(), True),
        StructField("ARTICLE", StringType(), True),
        StructField("MOVE_ORDER_LINE_ID", StringType(), True),
        StructField("LOCATION_FROM_NAME", StringType(), True),
        StructField("LOCATION_FROM_ID", StringType(), True),
        StructField("LOCATION_TO_NAME", StringType(), True),
        StructField("LOCATION_TO_ID", StringType(), True),
        StructField("APPROVED_QUANTITY", StringType(), True),
        StructField("RESERVED_QUANTITY", StringType(), True),
        StructField("DISPATCHED_QUANTITY", StringType(), True),
        StructField("RECEIVED_QUANTITY", StringType(), True),
        StructField("TO_BE_DISPATCHED", StringType(), True)])
    
    