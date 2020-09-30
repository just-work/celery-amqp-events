from celery.app.task import Task
from celery.exceptions import Reject

from demo import events
from demo.celery import app


@events.event_occured.handler
def on_event_occured(value: str) -> None:
    print(f"event occured: {value}")
    if value == 'reject':
        raise ValueError('reject')


@app.handler(events.number_is_odd.name, bind=True)
def on_number_is_odd(self: Task, number: int) -> None:
    if self.request.retries < 5:
        # retry task once
        raise ValueError(number)
    print(f"number {number} is odd {self.request.correlation_id}")


@app.handler(events.number_is_even.name)
class NumberEvenHandler(Task):
    # noinspection PyMethodMayBeStatic
    def run(self, number: int) -> None:
        print(f"number {number} is even")
