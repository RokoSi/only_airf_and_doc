import string


def validator_pass(password: str) -> bool:
    """
    Валидирует пароль.
    :param password: Пароль для валидации.
    :return: True - если пароль валиден, False - если пароль не валиден.
    """
    return (
        any(char in string.punctuation for char in password)
        and any(char in string.digits for char in password)
        and any(char in string.ascii_uppercase for char in password)
        and any(char in string.ascii_lowercase for char in password)
    )
