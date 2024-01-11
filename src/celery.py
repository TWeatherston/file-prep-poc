from celery import Celery

from .settings import settings


app = Celery(
    "data-preparer", broker=settings.broker_url.unicode_string(), include=["src.tasks"]
)


if __name__ == "__main__":
    app.start()
