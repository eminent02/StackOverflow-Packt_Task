from datetime import datetime, timedelta, date
import json
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
# from airflow.operators.postgres_operator import PostgresOperator
import requests
import psycopg2
from trending_tags import fetch_trending_tags
from unanswered_questions import fetch_unanswered_questions
from top_questions_by_tag import fetch_top_questions
import os
from dotenv import load_dotenv
load_dotenv()

# redshift conn details
redshift_hostname = os.getenv("redshift_hostname")
redshift_port = os.getenv("redshift_port")
redshift_user = os.getenv("redshift_user")
redshift_database = os.getenv("redshift_database")
redshift_password = os.getenv("redshift_password")


# Stack Overflow API endpoint and parameters
api_url = "https://api.stackexchange.com/2.3"
api_params = {
    "site": "stackoverflow",
    "pagesize": 100
}
# DEFINE DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 7, 4),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    
}

dag = DAG(
    "stackoverflow_pipeline",
    default_args=default_args,
    description="Data pipeline to fetch Stack Overflow data",
    schedule_interval='@once',#"0 0 1 */1 *",  # Run monthly on the 1st day
    catchup = False
)


def fetch_all_time_popular_tags():
    # Fetch top 10 all-time popular tags
    api_params["sort"] = "popular"
    api_params["pagesize"] = 10
    response = requests.get(api_url + "/tags", params=api_params)
    tags = response.json().get("items", [])
    return tags


def store_tags(tags):
    # Store the fetched tags in Redshift
    insert_query = "CREATE TABLE IF NOT EXISTS tags (tag_name VARCHAR(50), tag_count INTEGER, month VARCHAR(7));INSERT INTO tags (tag_name, tag_count, month) VALUES %s"
    previous_month = (date.today().replace(day=1) - timedelta(days=1)).strftime("%Y-%m")
    tag_values = [(tag["name"], tag["count"], previous_month) for tag in tags]
    redshift_conn = psycopg2.connect(
        host=redshift_hostname,
        port=redshift_port,
        database=redshift_database,
        user=redshift_user,
        password=redshift_password
    )
    with redshift_conn:
        with redshift_conn.cursor() as cursor:
            psycopg2.extras.execute_values(cursor, insert_query, tag_values)
    redshift_conn.close()

def store_questions(questions):
    # Store the fetched questions in Redshift
    insert_query = "CREATE TABLE IF NOT EXISTS questions (question_id INTEGER, title VARCHAR(1000), view_count integer, creation_date TIMESTAMP, score INTEGER, link VARCHAR(1000), answer_count INTEGER, last_activity_date TIMESTAMP, is_answered BOOLEAN);INSERT INTO questions (question_id, title, view_count, creation_date, score, link, answer_count, last_activity_date, is_answered) VALUES %s"
    question_values = [(q["question_id"], q["title"], q["view_count"], datetime.utcfromtimestamp(int(q["creation_date"])).strftime('%Y-%m-%d %H:%M:%S'), q["score"], q["link"],q["answer_count"],datetime.utcfromtimestamp(int(q["last_activity_date"])).strftime('%Y-%m-%d %H:%M:%S'), q["is_answered"]) for q in questions]
    redshift_conn = psycopg2.connect(
        host=redshift_hostname,
        port=redshift_port,
        database=redshift_database,
        user=redshift_user,
        password=redshift_password
    )
    with redshift_conn:
        with redshift_conn.cursor() as cursor:
            psycopg2.extras.execute_values(cursor, insert_query, question_values)
    redshift_conn.close()


# TASKS
fetch_trending_tags_task = PythonOperator(
    task_id="fetch_trending_tags",
    python_callable=fetch_trending_tags,
    dag=dag,
)

fetch_all_time_popular_tags_task = PythonOperator(
    task_id="fetch_all_time_popular_tags",
    python_callable=fetch_all_time_popular_tags,
    dag=dag,
)

fetch_unanswered_questions_task = PythonOperator(
    task_id="fetch_unanswered_questions",
    python_callable=fetch_unanswered_questions,
    dag=dag,
)

store_trending_tags_task = PythonOperator(
    task_id="store_trending_tags",
    python_callable=store_tags,
    op_args=[fetch_trending_tags_task.output],
    dag=dag,
)

store_all_time_popular_tags_task = PythonOperator(
    task_id="store_all_time_popular_tags",
    python_callable=store_tags,
    op_args=[fetch_all_time_popular_tags_task.output],
    dag=dag,
)

store_unanswered_questions_task = PythonOperator(
    task_id="store_unanswered_questions",
    python_callable=store_questions,
    op_args=[fetch_unanswered_questions_task.output],
    dag=dag,
)

fetch_trending_tags_task >> store_trending_tags_task
fetch_all_time_popular_tags_task >> store_all_time_popular_tags_task
fetch_unanswered_questions_task >> store_unanswered_questions_task

top_10_all_time_popular_tags = fetch_all_time_popular_tags()[:10]

for i, tag in enumerate(top_10_all_time_popular_tags):
    fetch_top_questions_task = PythonOperator(
        task_id=f"fetch_top_questions_{i}",
        python_callable=fetch_top_questions,
        op_args=[tag["name"]],
        dag=dag,
    )
    
    store_top_questions_task = PythonOperator(
        task_id=f"store_top_questions_{i}",
        python_callable=store_questions,
        op_args=[fetch_top_questions_task.output],
        dag=dag,
    )
    
    fetch_all_time_popular_tags_task >> fetch_top_questions_task
    fetch_top_questions_task >> store_top_questions_task


