import logging
from typing import Callable, Union, Tuple, Any, List, Optional
import psycopg2
from psycopg2 import OperationalError, ProgrammingError, DatabaseError
from .ddl import ddl_use_string


log = logging.getLogger(__name__)


def decorator_get_users_db(func: Callable) -> Callable:
    def wrapper(
        settings, query: str, param: Optional[Tuple[Any, ...]] = None
    ) -> Union[List[dict], int, bool]:
        """
        Обработка ошибок.
        :param settings: Данные для подключения к бд
        :param query:   Запрос SQL
        :param param: Опционально, параметры для query
        :return: dict - Возвращает, если это предусмотрено запросом, bool - если ошибка
        """
        try:
            return func(settings, query, param)
        except OperationalError as oe:
            log.error(f"Ошибка подключения к базе данных: {oe}")
            return False
        except psycopg2.errors.UniqueViolation as e:
            log.info(f"Ошибка уникального ограничения:{e} Данные не будут добавлены")
            return False
        except ProgrammingError as pe:
            if str(pe) != 'ОШИБКА:  отношение "contact_details" уже существует\n':
                log.error(f"Ошибка в SQL запросе: {pe}")
                return False
        except DatabaseError as de:
            log.error(f"Ошибка базы данных: {de}")
            return False
        except Exception as e:
            log.error(f"Произошла ошибка: {e}")
            return False
        return False

    return wrapper


@decorator_get_users_db
def connect_db(
    setting, query: str, param: Optional[Tuple[Any, ...]] = None
) -> list[tuple[Any, ...]] | int | bool:
    """
    Подключение к бд
    :param setting: Данные для подключения
    :param query: Запрос
    :param param: Опционально, параметры для запроса
    :return: list - если есть что возвращаешь, int - если нечего вернуть
    """
    try:
        with psycopg2.connect(
            host=setting.host,
            user=setting.user,
            password=setting.password,
            database=setting.db,
        ) as connection:
            connection.autocommit = True
        with connection.cursor() as cursor:
            cursor.execute(query, param)

            if cursor.description is not None:
                return cursor.fetchall()
            else:
                return cursor.rowcount
    except Exception as e:
        log.error(f"Ошибка при выполнении запроса: {e}")
        return False


def create_db(setting) -> bool:
    """
    Создание таблиц, если их нет
    :param setting: Данные для подключения
    :return: True - если таблицы были созданы,
     False - если не создавались таблицы или есть ошибки
    """
    query: str = (
        "SELECT COUNT(*) FROM pg_catalog.pg_tables"
        " WHERE schemaname NOT IN ('pg_catalog',"
        "'information_schema')"
    )

    count_table: Union[List[Tuple[int]], bool] = connect_db(setting, query)
    if isinstance(count_table, list):
        if count_table[0][0] == 0:
            try:
                connect_db(setting, ddl_use_string())
                return True
            except FileNotFoundError as fe:
                log.error(f"Ошибка пути: {fe}")
                return False
        return False
    else:
        return False
