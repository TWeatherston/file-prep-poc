from faststream.rabbit.fastapi import RabbitRouter
from faststream.rabbit import ExchangeType, RabbitExchange, RabbitQueue

from . import tasks, schema
from .settings import settings

router = RabbitRouter(settings.broker_url.unicode_string())
exch = RabbitExchange("exchange", auto_delete=True, type=ExchangeType.TOPIC)
queue_1 = RabbitQueue("file-prep", auto_delete=True, routing_key="adc.data_collected")


@router.subscriber(
    queue_1,
    exch,
    title=queue_1.routing_key,
    description="Running file preparation Celery task",
)
async def prepare_file(data: schema.PrepareFile):
    tasks.prepare_file.delay(**data.model_dump())
