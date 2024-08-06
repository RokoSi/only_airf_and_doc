from pydantic import EmailStr, ValidationError, BaseModel


class EmailModel(BaseModel):
    email: EmailStr


def validator_email(email: str) -> bool:
    """
    Валидирует email
    :param email: (str) email для проверки
    :return:True - если email валиден, False - если email не валиден
    """
    try:
        EmailModel(email=email)
        return True
    except ValidationError:
        return False
