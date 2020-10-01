from typing import Optional

from celery.app.task import Task

from demo import events
from demo.celery import app


@events.event_occured.handler(bind=True)
def on_event_occured(self: Task,
                     value: str,
                     countdown: Optional[int] = None) -> None:
    if value == 'retry':
        self.retry(countdown=countdown)
    if isinstance(value, Exception):
        raise value


@app.handler(events.number_is_odd.name)
def on_number_is_odd(number: int) -> None:
    if number % 2 == 1:
        raise ValueError(number)


@app.handler(events.number_is_even.name)
class NumberEvenHandler(Task):
    # noinspection PyMethodMayBeStatic
    def run(self, number: int) -> None:
        print(f"number {number} is even")
