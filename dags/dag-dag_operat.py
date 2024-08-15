import csv
import logging
import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

log_dir = os.path.join(os.getcwd(), "logs")
log_file = os.path.join(log_dir, "logfile.log")
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(
    filename=log_file,
    filemode="a",
    encoding="utf-8",
    level=logging.INFO,
    format="'%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%d/%m/%Y %I:%M:%S %p",
)
log = logging.getLogger(__name__)


def get_user(**kwargs):
    from get_user import get_users_url

    from airflow.models import Variable

    users = get_users_url(1, Variable.get("url"))
    file_path = os.path.join("./ods", "raw_data.csv")
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    with open(file_path, "w", newline="") as csvfile:
        fieldnames = [
            "gender",
            "name_title",
            "name_first",
            "name_last",
            "age",
            "nat",
            "email",
            "username",
            "password",
            "password_md5",
            "valid",
            "city",
            "state",
            "country",
            "street_name",
            "street_number",
            "postcode",
            "latitude",
            "longitude",
            "phone",
            "cell",
            "picture",
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for user in users:
            writer.writerow(
                {
                    "gender": user["gender"],
                    "name_title": user["name"]["title"],
                    "name_first": user["name"]["first"],
                    "name_last": user["name"]["last"],
                    "age": user["dob"]["age"],
                    "nat": user["nat"],
                    "email": user["email"],
                    "username": user["login"]["username"],
                    "password": user["login"]["password"],
                    "password_md5": user["login"]["md5"],
                    "valid": True,
                    "city": user["location"]["city"],
                    "state": user["location"]["state"],
                    "country": user["location"]["country"],
                    "street_name": user["location"]["street"]["name"],
                    "street_number": user["location"]["street"]["number"],
                    "postcode": user["location"]["postcode"],
                    "latitude": user["location"]["coordinates"]["latitude"],
                    "longitude": user["location"]["coordinates"]["longitude"],
                    "phone": user["phone"],
                    "cell": user["cell"],
                    "picture": user["picture"]["large"],
                }
            )

    kwargs["ti"].xcom_push(key="raw_csv_file", value=file_path)


def read_from_csv(ti):
    raw_file_path = ti.xcom_pull(task_ids="get_user", key="raw_csv_file")
    ods_file_path = os.path.join("./ods", "ods_data.csv")
    os.makedirs(os.path.dirname(ods_file_path), exist_ok=True)

    users = []

    with open(raw_file_path, "r") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            from validators import validator_pass
            row["valid"] = validator_pass(row)
            users.append(row)

    with open(ods_file_path, "w", newline="") as csvfile:
        fieldnames = users[0].keys()
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(users)

        ti.xcom_push(key="ods_csv_file", value=ods_file_path)


def save_data_to_postgres(ti):
    ods_file_path = ti.xcom_pull(task_ids="read_csv_save_ods", key="ods_csv_file")

    if not ods_file_path or not os.path.exists(ods_file_path):
        raise ValueError(f"Файл {ods_file_path} не найден или не указан")

    users = []
    with open(ods_file_path, "r") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            users.append(row)

    from airflow.providers.postgres.hooks.postgres import PostgresHook

    pg_hook = PostgresHook(postgres_conn_id="postgres_connection")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    for user in users:
        try:
            cursor.execute(
                """INSERT INTO cities(city, state, country)
            VALUES (%s, %s, %s ) RETURNING city_id"""
            , (
                user["city"],
                user["state"],
                user["country"],
            )
            )

            city_id = cursor.fetchone()[0]
        except Exception as e:
            print(f"Ошибка: {e}")

        cursor.execute(
            """
            INSERT INTO users (gender, name_title, name_first, name_last, age, nat)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING user_id
            """,
            (
                user["gender"],
                user["name_title"],
                user["name_first"],
                user["name_last"],
                user["age"],
                user["nat"],
            ),
        )
        user_id = cursor.fetchone()[0]

        cursor.execute(
            """
            INSERT INTO contact_details (user_id, phone, cell)
            VALUES (%s, %s, %s)
            """,
            (user_id, user["phone"], user["cell"]),
        )

        cursor.execute(
            """
            INSERT INTO media_data (user_id, picture)
            VALUES (%s, %s)
            """,
            (user_id, user["picture"]),
        )

        cursor.execute(
            """
            INSERT INTO registration_data (user_id, email, username, password, password_md5, password_validation)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                user_id,
                user["email"],
                user["username"],
                user["password"],
                user["password_md5"],
                user["valid"],
            ),
        )

        cursor.execute(
            """
            INSERT INTO locations (user_id, city_id, street_name, street_number, postcode, latitude, longitude)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                user_id,
                city_id,
                user["street_name"],
                user["street_number"],
                user["postcode"],
                user["latitude"],
                user["longitude"],
            ),
        )

    conn.commit()
    cursor.close()


args = {
    "owner": "Chernyshev-Pridvorov",
    "depends_on_past": False,
    "start_date": datetime(2023, 8, 5),
    "retries": 1,
    # 'provide_context': True
}
with DAG(
        "test_de_airf",
        description="test1",
        schedule_interval="*/1 * * * *",
        catchup=False,  # TODO
        default_args=args,
) as dag:
    get_users = PythonOperator(
        task_id="get_user",
        python_callable=get_user,
    )

    read_and_save_csv = PythonOperator(
        task_id="read_csv_save_ods",
        python_callable=read_from_csv,
    )
    save_data = PythonOperator(
        task_id="save_data_to_postgres",
        python_callable=save_data_to_postgres,
    )

get_users >> read_and_save_csv >> save_data
