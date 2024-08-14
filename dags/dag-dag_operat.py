import csv
import logging
import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

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
    file_path = os.path.join("./ods", "users_data.csv")
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

    kwargs["ti"].xcom_push(key="users_csv_file", value=file_path)


def read_from_csv(ti):
    from validators import validator_pass

    file_path = ti.xcom_pull(task_ids="get_user", key="users_csv_file")

    if not file_path or not os.path.exists(file_path):
        print("путь к файлу:", ti.xcom_pull(task_ids="save_data_to_csv", key="users_csv_file"))
        raise ValueError(f"Файл {file_path} не найден или не указан")

    users = []

    with open(file_path, "r") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            users.append(row)

    from airflow.providers.postgres.hooks.postgres import PostgresHook

    pg_hook = PostgresHook(postgres_conn_id="postgres_connection")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    for user in users:
        from pprint import pprint

        pprint(user)
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

        # Вставка в таблицу cities
        cursor.execute(
            """
            INSERT INTO cities (city, state, country)
            VALUES (%s, %s, %s)
            ON CONFLICT (city, state, country) DO NOTHING
            RETURNING city_id
        """,
            (

                user["city"],
                user["state"],
                user["country"],
            ),
        )
        city_id = cursor.fetchone()[0] if cursor.rowcount > 0 else None

        cursor.execute(
            """
            INSERT INTO contact_details (user_id, phone, cell)
            VALUES (%s, %s, %s)
        """,
            (user_id,
             user["phone"],
             user["cell"]),
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
                validator_pass(user["password"]),
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


ddl: str = """

    CREATE table if not EXISTS users(
    user_id      INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    gender       VARCHAR(255),
    name_title   VARCHAR(255),
    name_first   VARCHAR(255),
    name_last    VARCHAR(255),
    age 		 INT,
    nat          VARCHAR(255),
    created_dttm TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_dttm TIMESTAMP DEFAULT CURRENT_TIMESTAMP
   );

-- Создание таблицы contact_details
CREATE table if not EXISTS contact_details(
    user_id      INT NOT NULL,
    phone        VARCHAR(255),
    cell         VARCHAR(255),
    created_dttm TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_dttm TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (user_id) REFERENCES users(user_id)
);

-- Создание таблицы media_data
CREATE table if not EXISTS media_data(
    user_id         INT NOT NULL,
    picture         VARCHAR(255),
    created_dttm    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_dttm    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (user_id) REFERENCES users(user_id)
);
CREATE table if not EXISTS registration_data(
    user_id             INT NOT NULL,
    email               VARCHAR(255),
    username            VARCHAR(255),
    password            VARCHAR(255),
    password_md5        VARCHAR(255),
    password_validation BOOLEAN,
    created_dttm        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_dttm        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (user_id) REFERENCES users(user_id)
);

-- Создание таблицы cities
CREATE table if not EXISTS cities(
    city_id 	 INT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    city 		 VARCHAR(255),
    state 		 VARCHAR(255),
    country 	 VARCHAR(255),
    created_dttm TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_dttm TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_city_state_country UNIQUE (city,state,country)

);

-- Создание таблицы locations
CREATE table if not EXISTS locations(
    user_id       INT NOT NULL,
    city_id       INT NOT NULL,
    street_name   VARCHAR(255),
    street_number INT,
    postcode      VARCHAR(255),
    latitude      FLOAT,
    longitude     FLOAT,
    created_dttm  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_dttm  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (user_id) REFERENCES users(user_id),
    FOREIGN KEY (city_id) REFERENCES cities(city_id)
);"""

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

    create_tables = PostgresOperator(
        task_id="create_tables",
        postgres_conn_id="postgres_connection",
        sql=ddl,
        dag=dag,
    )

save_data = PythonOperator(
    task_id="save_data_to_postgres",
    python_callable=read_from_csv,
)

get_users >> create_tables >> save_data
