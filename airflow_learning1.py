try:
    from datetime import timedelta
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from datetime import datetime

except Exception as e:
    print("Error  {} ".format(e))

def print_welcome_message(**context):
    print("Welcome to Airflow")
    first_number = 5
    second_number = 4
    context['ti'].xcom_push(key='numbers', value={'first_number': first_number, 'second_number': second_number})
    return "Welcome to Airflow"

def print_sum(**context):
    numbers = context.get("ti").xcom_pull(key="numbers")
    print('@'*66)
    print("The sum is {}".format(sum(numbers.values())))
    print('@'*66)

def print_end_message(**context):
    print("End of Airflow")
    return "End of Airflow"

with DAG(
        dag_id="airflow_learning1",
        schedule_interval="@daily",
        default_args={
            "owner": "airflow",
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
            "start_date": datetime(2021, 1, 1),
        },
        catchup=False) as f:

    print_welcome_message = PythonOperator(
        task_id="print_welcome_message",
        python_callable=print_welcome_message,
        provide_context=True,
    )

    print_sum = PythonOperator(
        task_id="print_sum",
        python_callable=print_sum,
        provide_context=True,
    )

    print_end_message = PythonOperator(
        task_id="print_end_message",
        python_callable=print_end_message,
        provide_context=True,
    )

print_welcome_message >> print_sum >> print_end_message