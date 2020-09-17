from demo.events import *


def send_event():
    event_occured('event')
    r = number_is_odd(1)
    number_is_even(2)

    return repr(r)


if __name__ == '__main__':
    print(send_event())
