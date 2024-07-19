import datetime
import json
from airflow import DAG
from airflow import models
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime

# Parametros gerais
bucket_path = "gs://diretorio_temporario"
project_id = "PROJECT"


# Parametros de Dataform
dataform_token = 'TOKEN_DE_AUTENTICACAO_DA_API_DO_DATAFORM'


# Parametros de banco de dados
database_url = "jdbc:oracle:thin:@(description=(address=(host=172.24.196.41)(protocol=tcp)(port=1521))(connect_data=(SERVICE_NAME=pconsprsv)))"
driver_class = "oracle.jdbc.driver.OracleDriver"
driver_jar = "gs://bucket_do_drive/ojdbc8.jar"
db_user = "user_do_banco"
db_pass = "password_do_banco"


# Parametros de Dataflow
dataflow_template = "gs://databricks-dataflow-template/templates/JdbcToAvro"
dataflow_zone = "us-west1-c"
dataflow_subnet = "https://www.googleapis.com/compute/v1/projects/NOME_DO_PROJETO/regions/us-west1/subnetworks/NOME_DA_SUBNET"
dataflow_network = "projects/NOME_DO_PROJETO/global/networks/NOME_DA_REDE"
dataflow_service_acc = "XXXXXXXXXXXXXX-compute@developer.gserviceaccount.com"
dataflow_ip_config = "WORKER_IP_PRIVATE"
dataflow_num_workers = 3
dataflow_machine_type = "n1-standard-1"

default_args = {
    "start_date": days_ago(1),
    "dataflow_default_options": {
        "project": project_id,
        "tempLocation": bucket_path + "/tmp/",
        "zone": dataflow_zone,
        "subnetwork": dataflow_subnet,
        "network": dataflow_network,
        "serviceAccountEmail": dataflow_service_acc,
        "ipConfiguration": dataflow_ip_config,
        "numWorkers": dataflow_num_workers,
        "machineType": dataflow_machine_type,
    },
}

dag = DAG(
    "NOME_DA_DAG",
    default_args=default_args,
    schedule_interval=None,
)

# Data de carga
data_ref = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')

list_dict_param_jobs_load = [
    { "table_name": "TABELA1"               , "load_type": "inc", "column_filter": "COLUNA_DE_FILTRO" },
    { "table_name": "TABELA2"               , "load_type": "inc", "column_filter": "COLUNA_DE_FILTRO2" },
    { "table_name": "TABELA3"               , "load_type": "full" },
    { "table_name": "TABELA4"               , "load_type": "full" },
]


call_dataform_job = SimpleHttpOperator(
    task_id="call_dataform_job",
    http_conn_id="dataform_default",
    method="POST",
    headers={"Authorization": "bearer " + dataform_token},
    data=json.dumps({"scheduleName": "run_all_tables"}),
    endpoint="",
    dag=dag
)

for param_dict in list_dict_param_jobs_load:

    if 'column_filter' in param_dict.keys():
        filter = " WHERE " + param_dict['column_filter'] + " = TO_DATE('" + data_ref + "','YYYYMMDD')"
    else:
        filter = ''

    clear_landing_table = BigQueryOperator(
        task_id="clear_landing_{table_name}".format(table_name=param_dict["table_name"]),
        sql="truncate table " + project_id + ".poc_landing." + param_dict["table_name"],
        use_legacy_sql=False,
        priority="BATCH",
        dag=dag,
    )

    load_landing_table = DataflowTemplatedJobStartOperator(
        task_id="load_landing_{table_name}".format(table_name=param_dict["table_name"]),
        template="gs://dataflow-templates/latest/Jdbc_to_BigQuery",
        parameters={
            "connectionURL": database_url,
            "driverClassName": driver_class,
            "connectionProperties": "defaultRowPrefetch=" + param_dict.setdefault('row_prefetch', '20000'),
            "query": "SELECT * FROM DATASET." + param_dict["table_name"].upper() + filter,
            "outputTable": project_id + ":poc_landing." + param_dict["table_name"],
            "driverJars": driver_jar,
            "bigQueryLoadingTemporaryDirectory": bucket_path + "/tmp",
            "username": db_user,
            "password": db_pass
        },
        dag=dag,
    )

    clear_landing_table >> load_landing_table >> call_dataform_job