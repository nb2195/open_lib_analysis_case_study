# *************************************************************************
# ***              WRAPPER CREATION LOGIC                               ***
# ***              DEVELOPED DATE: 2022/03/01                           ***
# ***              AUTHOR : Neelkantha Banerjee                         ***
# ***              DESCRIPTION: wrapper.py                              ***
# ***              Trigger script that launches                         ***
# ***              spark-submit command                                 ***
# ***              SAMPLE COMMAND :  `python wrapper.py {config}`       ***
# *************************************************************************

import os
import sys
import yaml
import time


def main(config_loc):
    with open(config_loc) as json_file:
        config_main = yaml.load(json_file)
    # building the spark submit command
    spark_submit_command = ""
    if config_main["spark_config"]["deploy_mode"] == "client":
        # spark submit command for client mode
        spark_submit_command = """
                    spark-submit --name "{application_name}" --master yarn --deploy-mode {deploy_mode} --conf spark.yarn.keytab={keytab_file} --conf spark.yarn.principal={principal} --conf spark.yarn.appMasterEnv.HADOOP_USER_NAME={service_account}  --conf spark.yarn.queue='{queue}' {extra_pyspark_conf} --num-executors {num_executors} --executor-memory {exec_memory} --driver-memory {driver_mem} --executor-cores {num_cores} --files {config_loc} --py-files {code_base} {main_file} {json_name}
                    """.format(
            application_name=config_main["spark_config"]["application_name"],
            deploy_mode=config_main["spark_config"]["deploy_mode"],
            num_executors=config_main["spark_config"]["num_executors"],
            exec_memory=config_main["spark_config"]["executor_memory"],
            driver_mem=config_main["spark_config"]["driver_memory"],
            num_cores=config_main["spark_config"]["executor_cores"],
            code_base="{0}".format(config_main["code"]),
            main_file="{0}".format(config_main["driver_name"]),
            keytab_file=config_main["sa_keytab_file"],
            service_account=config_main["service_account"],
            principal=config_main["principal"],
            queue=config_main["queue"],
            extra_pyspark_conf=config_main["spark_config"]["extra_pyspark_conf"],
            json_name=config_loc,
            config_loc=config_loc
        )
    elif config_main["spark_config"]["deploy_mode"] == "cluster":
        # spark submit command for cluster mode
        spark_submit_command = """
                    spark-submit --name "{application_name}" --master yarn --deploy-mode {deploy_mode} --conf spark.yarn.keytab={keytab_file} --conf spark.yarn.principal={principal} --conf spark.yarn.appMasterEnv.HADOOP_USER_NAME={service_account}  --conf spark.yarn.queue='{queue}' {extra_pyspark_conf} --num-executors {num_executors} --executor-memory {exec_memory} --driver-memory {driver_mem} --executor-cores {num_cores} --files {config_loc} --py-files {code_base} {main_file} {json_name}
                    """.format(
            application_name=config_main["spark_config"]["application_name"],
            deploy_mode=config_main["spark_config"]["deploy_mode"],
            num_executors=config_main["spark_config"]["num_executors"],
            exec_memory=config_main["spark_config"]["executor_memory"],
            driver_mem=config_main["spark_config"]["driver_memory"],
            num_cores=config_main["spark_config"]["executor_cores"],
            code_base="{0}".format(config_main["code"]),
            main_file="{0}".format(config_main["driver_name"]),
            keytab_file=config_main["sa_keytab_file"],
            service_account=config_main["service_account"],
            principal=config_main["principal"],
            queue=config_main["queue"],
            extra_pyspark_conf=config_main["spark_config"]["extra_pyspark_conf"],
            json_name=config_loc.split("/")[-1],
            config_loc=config_loc
        )
    else:
        print("Please enter the correct deploy mode")
    try:
        print(spark_submit_command)
        time.sleep(5)
        os.system(spark_submit_command)
    except OSError:
        raise ValueError("spark submit failed")



if __name__ == "__main__":
    config_loc = sys.argv[1]
    main(config_loc)
