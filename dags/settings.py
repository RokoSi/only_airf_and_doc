from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=("../.env.dev", "../src/.env.secret"),
        env_file_encoding="utf-8",
    )
    url: str
    user_kafka: str
    password_kafka: str
    topic: str


settings = Settings()

if __name__ == "__main__":
    print(settings)
#     import os 
#     file_name = '.env.dev'
#     directories = ['.', '..', '../src']
#     for directory in directories:
#         file_path = os.path.join(directory, file_name)
#         if os.path.isfile(file_path):
#             print(f"Файл найден: {file_path}")
#             break
#     else:
#         print(f"Файл {file_name} не найден в указанных директориях.")
