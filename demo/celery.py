import celery
from amqp_events.celery import events_app

app: celery.Celery = events_app(
    'demo_consumer',
    broker_url='amqp://guest:guest@rabbitmq:5672/',
    imports=['demo.tasks'])
