from demo.celery import app


# noinspection PyUnusedLocal
@app.event('demo.first.occured')
def event_occured(value: str):
    pass


@app.event('demo.number.even')
def number_is_even(number: int):
    if number % 2 != 0:
        raise ValueError(number)


@app.event('demo.number.odd')
def number_is_odd(number: int):
    if number % 2 == 0:
        raise ValueError(number)
