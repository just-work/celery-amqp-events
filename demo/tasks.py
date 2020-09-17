from celery import Task

from demo import events
from demo.celery import app


@events.event_occured.handler
def on_event_occured(value: str):
    print(f"event occured: {value}")


@app.handler(events.number_is_odd.name, bind=True)
def on_number_is_odd(self: Task, number: int):
    if self.request.retries == 0:
        # retry task once
        raise ValueError('1')
    print(f"number {number} is odd {self.request.correlation_id}")


@app.handler(events.number_is_even.name)
class NumberEvenHandler(Task):
    # noinspection PyMethodMayBeStatic
    def run(self, number: int):
        print(f"number {number} is even")
