from datetime import datetime, timedelta
import pytz
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.providers.amazon.aws.hooks.glue import GlueJobHook
#from airflow.providers.amazon.aws.operators.glue import AWSGlueJobOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

default_args = {
    'owner': 'Mark',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2024, 7, 10),
}

def list_files_in_s3(bucket_name, aws_conn_id="s3_conn",**kwargs):
    files_to_move = []
    tz = pytz.timezone('Australia/Adelaide')
    now = datetime.now(tz)
    print("now:",now)
    yesterday = now - timedelta(1)
    formatted_yesterday = yesterday.strftime('%Y%m%d')
    print("get_yesterday_local_time of yesterday:",yesterday.date())


    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    keys = s3_hook.list_keys(bucket_name=bucket_name)
    for key in keys:
        if key.endswith('.csv') and formatted_yesterday in key:
            files_to_move.append(key)
        else:
            print("No .csv file")
    print("current .csv file:",files_to_move)
    ti = kwargs['ti']
    ti.xcom_push(key='file_list', value=files_to_move)

def move_to_transient(src_bucket_name, dest_bucket_name, aws_conn_id,**kwargs):
    ti = kwargs['ti']
    file_key = ti.xcom_pull(task_ids='list_files_in_source_s3', key='file_list')
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    tz = pytz.timezone('Australia/Adelaide')
    now = datetime.now(tz)
    dest_key = now.strftime('%Y-%m-%d')


    for key in file_key:
        copy_source = {'Bucket': src_bucket_name, 'Key': key}
        print('copy_source:',copy_source)
        s3_hook.get_conn().copy_object(CopySource=copy_source, 
                                       Bucket=dest_bucket_name, 
                                       Key=f'{dest_key}/{key.split("/")[-1]}')

# def start_glue_job_raw(aws_conn_id="s3_conn",job_name = 'glue_job_video_raw_data'):
#     glue_hook = GlueJobHook(region_name = 'ap-southeast-2',aws_conn_id=aws_conn_id, job_name=job_name)
#     response = glue_hook.initialize_job()
#     print(response)
# 使用hook，我发现出现move_files_raw和move_files_curated两个job同时启动的问题。
# 因此使用airflow自带的GlueJobOperator，能够有效解决dependence的问题。
with DAG(
    dag_id='load_videodata_to_redshift_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    list_files = PythonOperator(
        task_id='list_files_in_source_s3',
        python_callable=list_files_in_s3,
        op_kwargs={'bucket_name': 'video-source-data'},
        provide_context=True
    )

    move_files = PythonOperator(
        task_id='move_files_to_transient',
        python_callable=move_to_transient,
        op_kwargs={'src_bucket_name': 'video-source-data',
                   'aws_conn_id': 's3_conn',
                   'dest_bucket_name': 'video-transient-bucket'    
                   },
        provide_context=True
    )

    # move_files_raw = PythonOperator(
    #     task_id='move_files_to_raw',
    #     python_callable=start_glue_job_raw,
    #     op_kwargs={'job_name': 'glue_job_video_raw_data'},
    #     provide_context=True
    # )

    # move_files_curated = PythonOperator(
    #     task_id='move_files_to_curated',
    #     python_callable=start_glue_job_raw,
    #     op_kwargs={'job_name': "glue_job_video_curated_data"},
    #     provide_context=True
    # )
    start_glue_job_raw = GlueJobOperator(
        task_id='start_glue_job_raw',
        job_name='glue_job_video_raw_data',
        aws_conn_id='s3_conn',
        region_name='ap-southeast-2'
    )

    start_glue_job_curated = GlueJobOperator(
        task_id='start_glue_job_curated',
        job_name='glue_job_video_curated_data',
        aws_conn_id='s3_conn',
        region_name='ap-southeast-2'
    )

################### move data to redshift part #######################
import psycopg2
def load_json_to_redshift():
    # config
    s3_bucket = 'video-curated-bucket'
    s3_prefix = 'semi-structure/'
    json_data_file = 'user_top10.json'
    jsonpaths_file = 'jsonpath.json'
    region = 'ap-southeast-2'
    iam_role = 'arn:aws:iam::471112748839:role/service-role/AmazonRedshift-CommandsAccessRole-20240528T153201'
    redshift_host = 'video-etl-workgroup.471112748839.ap-southeast-2.redshift-serverless.amazonaws.com'
    redshift_port = 5439
    redshift_dbname = 'video-test'
    redshift_user = ''
    redshift_password = ''

    try:
        conn = psycopg2.connect(
            host=redshift_host,
            port=redshift_port,
            dbname=redshift_dbname,
            user=redshift_user,
            password=redshift_password
        )
        cur = conn.cursor()

        # delete exsited table 
        drop_table_query = f'DROP TABLE IF EXISTS "video-test".public.user;'
        cur.execute(drop_table_query)

        # create new table
        create_table_query = '''
        CREATE TABLE "video-test".public.user (
            id VARCHAR,
            firstName VARCHAR,
            lastName VARCHAR,
            jobHistory VARCHAR(1024)
        );
        '''
        cur.execute(create_table_query)

        # use COPY load data
        copy_command = f'''
        COPY "video-test".public.user
        FROM 's3://{s3_bucket}/{s3_prefix}{json_data_file}'
        IAM_ROLE '{iam_role}'
        FORMAT AS JSON 's3://{s3_bucket}/{s3_prefix}{jsonpaths_file}'
        REGION AS 'ap-southeast-2';
        '''
        cur.execute(copy_command)

        conn.commit()

        # stop conn
        cur.close()
        conn.close()
        print("Data loaded successfully.")
    except psycopg2.OperationalError as e:
        print("Unable to connect to the Redshift server.")
        print(e)
    except Exception as e:
        print("An error occurred.")
        print(e)

json_to_redshift_task = PythonOperator(
    task_id='load_json_to_redshift',
    python_callable=load_json_to_redshift,
    dag=dag,
)



def load_csv_to_redshift():
    s3_conn_id = 's3_conn'  # connection
    s3_bucket = 'video-curated-bucket'
    s3_prefix = 'data-warehouse/'
    region = 'ap-southeast-2'
    iam_role = 'arn:aws:iam::471112748839:role/service-role/AmazonRedshift-CommandsAccessRole-20240528T153201'
    redshift_host = 'video-etl-workgroup.471112748839.ap-southeast-2.redshift-serverless.amazonaws.com'
    redshift_port = 5439
    redshift_dbname = 'video-test'
    redshift_user = ''
    redshift_password = ''

    current_date = datetime.now()
    yesterday_date = current_date - timedelta(days=1)
    yesterday_date_str = yesterday_date.strftime('%Y-%m-%d')
    current_date_str = current_date.strftime('%Y-%m-%d')
    # 使用 S3Hook 获取 S3 客户端
    s3_hook = S3Hook(aws_conn_id=s3_conn_id)
    s3 = s3_hook.get_conn()

    tables = {
        "FactVideo": {
            "create_table": '''
            CREATE TABLE "video-test".public.FactVideo (
                record_id INTEGER,
                user_id VARCHAR(256),
                datetime TIMESTAMP WITHOUT TIME ZONE,
                topic_id INTEGER,
                videotopic VARCHAR(256),
                events VARCHAR(256)
            )
            DISTSTYLE EVEN;
            ''',
            "s3_prefix": f"{s3_prefix}factvideo/{current_date_str}/"
        },
        "DimDate": {
            "create_table": '''
            CREATE TABLE "video-test".public.DimDate (
                datekey INTEGER,
                datetime TIMESTAMP WITHOUT TIME ZONE,
                hour INTEGER,
                minute INTEGER,
                dayofweek INTEGER,
                month INTEGER,
                quarter INTEGER,
                year INTEGER
            )
            DISTSTYLE ALL;
            ''',
            "s3_prefix": f"{s3_prefix}dimdate/{current_date_str}/"
        },
        "DimVideoTopic": {
            "create_table": '''
            CREATE TABLE "video-test".public.DimVideoTopic (
                topic_id INTEGER,
                videotopic VARCHAR(512),
                videoname_id INTEGER,
                videotype_id INTEGER,
                platform_id INTEGER
            )
            DISTSTYLE ALL;
            ''',
            "s3_prefix": f"{s3_prefix}dimvideotopic/{current_date_str}/"
        },
        "DimPlatform": {
            "create_table": '''
            CREATE TABLE "video-test".public.DimPlatform (
                platform_id INTEGER,
                platform VARCHAR(64)
            )
            DISTSTYLE ALL;
            ''',
            "s3_prefix": f"{s3_prefix}dimplatform/{current_date_str}/"
        },
        "DimVideoName": {
            "create_table": '''
            CREATE TABLE "video-test".public.DimVideoName (
                videoname_id INTEGER,
                videoname VARCHAR(512)
            )
            DISTSTYLE ALL;
            ''',
            "s3_prefix": f"{s3_prefix}dimvideoname/{current_date_str}/"
        },
        "DimVideoType": {
            "create_table": '''
            CREATE TABLE "video-test".public.DimVideoType (
                videotype_id INTEGER,
                videotype VARCHAR(64)
            )
            DISTSTYLE ALL;
            ''',
            "s3_prefix": f"{s3_prefix}dimvideotype/{current_date_str}/"
        }
    }

    try:
        conn = psycopg2.connect(
            host=redshift_host,
            port=redshift_port,
            dbname=redshift_dbname,
            user=redshift_user,
            password=redshift_password
        )
        cur = conn.cursor()

        for table_name, table_info in tables.items():
            print(table_name)
            print(table_info)
            drop_table_query = f'DROP TABLE IF EXISTS "video-test".public.{table_name};'
            cur.execute(drop_table_query)

            cur.execute(table_info["create_table"])

            response = s3.list_objects_v2(Bucket=s3_bucket, Prefix=table_info["s3_prefix"])
            csv_files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.csv')]

            for csv_file in csv_files:
                copy_command = f'''
                COPY "video-test".public.{table_name}
                FROM 's3://{s3_bucket}/{csv_file}'
                IAM_ROLE '{iam_role}'
                FORMAT AS CSV
                DELIMITER ','
                QUOTE '"'
                IGNOREHEADER 1
                MAXERROR 10
                REGION AS '{region}';
                '''
                cur.execute(copy_command)

        conn.commit()

        cur.close()
        conn.close()
        print("Data loaded successfully.")
    except psycopg2.OperationalError as e:
        print("Unable to connect to the Redshift server.")
        print(e)
    except Exception as e:
        print("An error occurred.")
        print(e)



csv_to_redshift_task = PythonOperator(
    task_id='load_csv_to_redshift',
    python_callable=load_csv_to_redshift,
    dag=dag,
)


#############  sequence #######################
list_files >> move_files >> start_glue_job_raw >> start_glue_job_curated >> [json_to_redshift_task,csv_to_redshift_task]
