from datetime import datetime as dt
from pyspark.sql import SparkSession

spark = None
logger = None
application_name = None
application_id = None

"""
    create_session:
    =====================================================================================
    Function: Creates a spark session and sets up a logger
    =====================================================================================
    Parameters:
        - **config : dictionary of config variables 
    =====================================================================================
"""
def create_session(**config):
    global spark
    global logger
    global application_name
    global application_id

    workflow_name = config['spark_config']['application_name']

    # initialize spark and logger
    now = dt.now()
    now = now.strftime("%Y%m%d%H%M%S")
    application_name = "data_engine_{workflow_name}_{now}".format(workflow_name=workflow_name, now=now)

    spark = SparkSession.builder \
        .appName(application_name) \
        .enableHiveSupport() \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    logger = spark._jvm.org.apache.log4j.LogManager.getLogger(application_name)
    application_id = spark.sparkContext.applicationId

    logger.warn("Application registered as {id}".format(id=application_id))
    return spark, logger
