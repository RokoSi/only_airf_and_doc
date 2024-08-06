import logging
from typing import Any, List, Union

import psycopg2
from .data_provider import connect_db


log = logging.getLogger(__name__)


def update_param_table_registration_data_db(
    setting, email: str, name_param: str, value: Any
) -> Union[List[dict], bool]:
    """
    Метод для обновления данных в таблице registration_data

    :param setting: Данные для подключения
    :param email: email пользователя
    :param name_param: имя атрибута, который надо изменить
    :param value: новое значение
    :return: dict - если данные изменены, False - если нет
    """
    query: str = f"UPDATE registration_data SET {name_param} = %s WHERE email = %s"
    param: tuple = (value, email)
    return connect_db(setting, query, param)


def update_param_table_media_data_db(
    setting, email: str, name_param: str, value: Any
) -> Union[List[dict], bool]:
    """
    Метод для обновления данных в таблице media_data

    :param setting: Данные для подключения
    :param email: email пользователя
    :param name_param: имя атрибута, который надо изменить
    :param value: новое значение
    :return: dict - если данные изменены, False - если нет
    """
    query: str = (
        f"""UPDATE media_data SET {name_param} = %s WHERE user_id = 
        (SELECT user_id FROM registration_data 
        WHERE email = %s)"""
    )
    param: tuple = (value, email)
    return connect_db(setting, query, param)


def update_param_table_contact_details_db(
    setting, email: str, name_param: str, value: Any
) -> Union[List[dict], bool]:
    """
    Метод для обновления данных в таблице contact_details

    :param setting: Данные для подключения
    :param email: email пользователя
    :param name_param: имя атрибута, который надо изменить
    :param value: новое значение
    :return: dict - если данные изменены, False - если нет
    """
    query: str = (
        f"""UPDATE contact_details SET {name_param} = %s WHERE user_id
        = ( SELECT user_id FROM 
        registration_data WHERE email = %s)"""
    )
    param: tuple = (value, email)
    return connect_db(setting, query, param)


def update_param_table_users_db(
    setting, email: str, name_param: str, value: Any
) -> Union[List[dict], bool]:
    """
    Метод для обновления данных в таблице users

    :param setting: Данные для подключения
    :param email: email пользователя
    :param name_param: имя атрибута, который надо изменить
    :param value: новое значение
    :return: dict - если данные изменены, False - если нет
    """
    query: str = (
        f"""UPDATE users SET {name_param} = %s WHERE user_id =
        ( SELECT user_id FROM registration_data WHERE 
        email = %s)"""
    )
    param: tuple = (value, email)
    return connect_db(setting, query, param)


def del_user(setting, email: str) -> bool:
    """
    Удаление пользователя по email
    :param setting: Данные для подключения
    :param email: email для удаления
    :return: True - если удалось удалить пользователя,
     False - если пользователь не удалось найти и удалить
    """
    query_check_email: str = "SELECT email FROM registration_data   WHERE email = %s"
    param_email: tuple = (email,)
    check = connect_db(setting, query_check_email, param_email)
    if bool(len(check)):
        query_del: str = f"""DELETE FROM registration_data
    WHERE email = %s;

    DELETE FROM locations
    WHERE user_id IN (
        SELECT user_id
        FROM registration_data
        WHERE email = %s
        LIMIT 1
    );

    DELETE FROM contact_details
    WHERE user_id IN (
        SELECT user_id
        FROM registration_data
        WHERE email = %s
        LIMIT 1
    );

    DELETE FROM media_data
    WHERE user_id IN (
        SELECT user_id
        FROM registration_data
        WHERE email = %s
        LIMIT 1
    );

    DELETE FROM users
    WHERE user_id IN (
        SELECT user_id
        FROM registration_data
        WHERE email = %s
        LIMIT 1
    );"""
        param: tuple = (email, email, email, email, email)
        try:
            connect_db(setting, query_del, param)
            return True
        except psycopg2.Error as e:
            log.error(f"Error deleting user: {e}")
            return False
    else:
        return False


def update_param_table_locations_db(
    setting, email: str, name_param: str, value: Union[str, float, int]
) -> Union[List[dict], bool]:
    """
    Метод для обновления данных в таблице locations
    :param setting: Данные для подключения к бд
    :param email: email пользователя
    :param name_param: имя атрибута, который надо изменить
    :param value: новое надо изменить
    :return: dict - если данные изменены, False - если нет
    """
    query: str = (
        f"UPDATE locations SET {name_param} = %s WHERE "
        f"user_id = (SELECT user_id FROM registration_data "
        f"WHERE email =%s)"
    )
    param: tuple = (value, email)
    return connect_db(setting, query, param)


def update_param_table_cities_db(
    setting, email: str, name_param: str, value: Any
) -> Union[List[dict], bool]:
    """
    Метод для обновления данных в таблице cities

    :param setting: Данные для подключения к бд
    :param email: email пользователя
    :param name_param: имя атрибута, который надо изменить
    :param value: новое значение
    :return: dict - если данные изменены, False - если нет
    """
    query: str = (
        f"UPDATE cities SET {name_param} = %s WHERE city_id ="
        f" (SELECT city_id FROM locations WHERE user_id "
        f"= (SELECT user_id FROM registration_data WHERE email = %s) )"
    )
    param: tuple = (value, email)
    return connect_db(setting, query, param)
