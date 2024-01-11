from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import AmqpDsn, SecretStr, BaseModel, HttpUrl


class Auth0(BaseModel):
    client_id: str
    client_secret: SecretStr
    audience: HttpUrl
    authorization_base_url: HttpUrl
    root_url: HttpUrl


class Settings(BaseSettings):
    output_dir: str = "file://output"
    broker_url: AmqpDsn = "amqp://guest:guest@rabbitmq:5672/"
    memcached_url: str = "memcached:11211"
    aws_access_key_id: str
    aws_secret_access_key: SecretStr

    auth0: Auth0

    model_config = SettingsConfigDict(
        env_file=".env.example", env_nested_delimiter="__"
    )


settings = Settings()
