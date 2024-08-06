import logging
from typing import Union, List, Dict
from .data_provider import connect_db

log = logging.getLogger(__name__)


def get_users_db(
    setting, password_validation: bool
) -> Union[List[Dict], bool]:
    """
    Получение user с помощью email

    :param setting: Данные для подключения к бд
    :param password_validation: True - для валидных
    пользователей,
     False - не валидные пользователи
    :return: dict - все данные о пользователе,
     False - если не удалось получить данные пользователе
    """
    query: str = (
        "SELECT gender, name_title, name_first, name_last,"
        " age, nat, phone, cell, picture, email, username, "
        "password, password_md5, password_validation, city,"
        " state, country, street_name, street_number, "
        "postcode, latitude, longitude FROM users JOIN"
        " contact_details ON users.user_id = "
        "contact_details.user_id JOIN media_data ON"
        " users.user_id = media_data.user_id JOIN "
        "registration_data ON users.user_id ="
        " registration_data.user_id  JOIN locations"
        " ON users.user_id = "
        "locations.user_id JOIN cities ON "
        "locations.city_id=cities.city_id  WHERE "
        "registration_data.password_validation = %s"
    )
    param: tuple = (password_validation,)
    return connect_db(setting, query, param)


def get_check_email(setting, email: str) -> bool:
    """
    Проверяет если ли такой пользователь в бд
    :param setting: Данные для подключения к бд
    :param email: email для проверки
    :return: True - если пользователь есть в бд,
     False - если такого пользователя нет
    """

    query: str = "SELECT * FROM REGISTRATION_DATA" " WHERE EMAIL = %s"
    email_tuple: tuple[str] = (email,)
    results: List = connect_db(setting, query, email_tuple)
    return bool(results)


def get_valid_user(setting, param_bool: bool):
    query: str = """SELECT * FROM users INNER JOIN 
    registration_data ON users.user_id = registration_data.user_id 
    WHERE registration_data.password_validation = %s"""

    param: tuple = (param_bool,)

    return connect_db(setting, query, param)
