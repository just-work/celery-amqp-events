from demo.events import event_occured


def send_event():
    event_occured('event')


if __name__ == '__main__':
    send_event()
