from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'cdac'
}

dag = DAG(
    dag_id='0001_cdac_final_project_data_cleaning',
    default_args=args,
    start_date=days_ago(1),
    schedule_interval='0 0 * * *',
)

part1 = BashOperator(
    task_id='part_1_csv',
    bash_command='python /home/rahulw/PycharmProjects/pythonProject/Airflow_DAGs/cdac_final_project/part1.py ',
    dag=dag)
    
part2 = BashOperator(
    task_id='part_2_json',
    bash_command='python /home/rahulw/PycharmProjects/pythonProject/Airflow_DAGs/cdac_final_project/part2.py ',
    dag=dag)
 
part3 = BashOperator(
    task_id='part_3_csv',
    bash_command='python /home/rahulw/PycharmProjects/pythonProject/Airflow_DAGs/cdac_final_project/part3.py ',
    dag=dag)  
     
part4 = BashOperator(
    task_id='part_4_xlsx',
    bash_command='python /home/rahulw/PycharmProjects/pythonProject/Airflow_DAGs/cdac_final_project/part4.py ',
    dag=dag)
    
part5 = BashOperator(
    task_id='part_5_json',
    bash_command='python /home/rahulw/PycharmProjects/pythonProject/Airflow_DAGs/cdac_final_project/part5.py ',
    dag=dag)


emailExtract = BashOperator(
    task_id='getEmailAttachment',
    bash_command='python /home/rahulw/PycharmProjects/pythonProject/Airflow_DAGs/cdac_final_project/getEmailAttachment_csv.py ',
    dag=dag)
    

Kaggle_data = BashOperator(task_id="Kaggle_data", bash_command="echo start", dag=dag)
    

Hdfs_startup = BashOperator(
    task_id='Start_Hdfs',
    bash_command='bash /home/rahulw/PycharmProjects/pythonProject/Airflow_DAGs/cdac_final_project/hdfs_start.sh ',
    dag=dag)
    
Load = BashOperator(task_id="Load_To_Hdfs", bash_command="echo start", dag=dag)

Synthetic_Data = BashOperator(task_id="Synthetic_Data", bash_command="echo start", dag=dag)

Mongodb_Database = BashOperator(task_id="Mongodb_Database", bash_command="echo start", dag=dag)

part6 = BashOperator(task_id="part_6", bash_command="echo start", dag=dag)
 
Hdfs_startup >> emailExtract >> part1 >> Load
Hdfs_startup >> Kaggle_data >> part2  >> Load
Hdfs_startup >> Kaggle_data >> part3  >> Load
Hdfs_startup >> Kaggle_data >> part4  >> Load
Hdfs_startup >> Synthetic_Data >> part5  >> Load
Hdfs_startup >> Mongodb_Database >> part6  >> Load




