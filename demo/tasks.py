from amqp_events.celery import app
from demo import events


@app.handler(events.event_occured.name)
def on_event_occured(value: str):
    print(f"event occured: {value}")


@events.number_is_odd.handler
def on_number_is_odd(number: int):
    print(f"number is odd")