from os import getenv as e
from amqp_events.celery import events_app

app = events_app(
    'demo_consumer',
    broker_url=e('EVENTS_BROKER_URL', 'amqp://guest:guest@rabbitmq:5672/'),
    imports=['demo.tasks'])
