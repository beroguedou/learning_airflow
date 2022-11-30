from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator

#from airflow.operators.python import PythonOperator
#from airflow.providers.amazon.aws.operators.s3 import S3ListOperator

#bucket_name = 'beranger-bucket-760254833251'
#PREFIX = 'data-source/'
#DELIMITER = '/'

#def _afficheur(ti):
#   output = ti.xcom_pull(key='return_value', task_ids='list_keys')
#    print('-------------------------------')
#    print(output[1])
#    print('-------------------------------')


with DAG(
    "predikt_churn_training_and_monitoring_dag", 
    schedule_interval="@daily", 
    start_date=datetime(2022, 1, 29),
    catchup=False
    ) as dag:

    clone_repository = EmptyOperator(task_id="clone_repository")

    #check_s3 = S3ListOperator(  # before this we need a role with fulls3access from ec2 instance where airflow is and port 8080 whitelist to see airflow ui
    #    task_id="list_keys",
    #    bucket=bucket_name,
    #    prefix=PREFIX,
    #    delimiter=DELIMITER
    #    )   

    #printer = PythonOperator(
    #    task_id='affiche_bucket', 
    #    python_callable=_afficheur
    #    )

    compute_and_store_features_train = EmptyOperator(task_id="compute_and_store_features_train")

    compute_and_store_features_inference = EmptyOperator(task_id="compute_and_store_features_inference")

    train_and_store_model = EmptyOperator(task_id="train_and_store_model")

    build_docker_image_and_push_to_registry = EmptyOperator(task_id="build_docker_image_and_push_to_registry")

    deploy_model = EmptyOperator(task_id="deploy_model")

    predict_and_store = EmptyOperator(task_id="predict_and_store")

    write_predictions_on_cassandra = EmptyOperator(task_id="write_predictions_on_cassandra")

    delete_model_deployment = EmptyOperator(task_id="delete_model_deployment")

    data_drift_detector = EmptyOperator(task_id="data_drift_detector")

    write_data_drift_metrics_on_druid = EmptyOperator(task_id="write_data_drift_metrics_on_druid")

    predictions_drift_detector = EmptyOperator(task_id="predictions_drift_detector")

    write_predictions_drift_metrics_on_druid = EmptyOperator(task_id="write_predictions_drift_metrics_on_druid")

    compute_and_store_backtesting_labels = EmptyOperator(task_id="compute_backtesting_labels")

    comparision_to_historical_predictions = EmptyOperator(task_id="comparision_to_historical_predictions")

    write_comparision_metrics_on_druid = EmptyOperator(task_id="write_comparision_metrics_on_druid")



clone_repository >> compute_and_store_backtesting_labels >> comparision_to_historical_predictions >> write_comparision_metrics_on_druid

clone_repository >> [compute_and_store_features_train, compute_and_store_features_inference] >> data_drift_detector >> write_data_drift_metrics_on_druid

compute_and_store_features_train >> train_and_store_model >> build_docker_image_and_push_to_registry >> deploy_model

[compute_and_store_features_inference, deploy_model] >> predict_and_store >> [write_predictions_on_cassandra, predictions_drift_detector]

predictions_drift_detector >> write_predictions_drift_metrics_on_druid

[write_predictions_drift_metrics_on_druid, write_predictions_on_cassandra] >> delete_model_deployment
