from amqp_events.celery import events_app

app = events_app(
    'demo_consumer',
    broker_url='amqp://guest:guest@rabbitmq:5672/',
    imports=['demo.tasks'])
