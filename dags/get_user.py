import json
import logging
from typing import Union, List, Dict
import requests
from settings import Settings

log = logging.getLogger(__name__)


def get_users_url(count_users: int, settings: Settings) -> Union[List[Dict], bool]:
    """
    Получение json с сайта
    :param count_users: количество пользователей
    :param settings: настройки, который хранят url для подключения
    :return: dict - если удалось получить json файл с сайта,
     bool - если не удалось получить json
    """
    try:
        if settings.url is None:
            log.error("URL для подключения отсутствует в настройках.")
            return False
        else:
            with requests.get(settings.url + str(count_users)) as response:
                if response.status_code == 200:
                    data: Dict = response.json()
                    return data["results"]
                else:
                    return False
    except (requests.exceptions.RequestException, json.decoder.JSONDecodeError) as e:
        log.error(f"Ошибка: {e}")
        return False
    except (KeyError, TypeError) as e:
        log.error(f"Ошибка: {e}")
        return False
