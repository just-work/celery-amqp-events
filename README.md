**Celery-AMQP-Events** is a library that implements voluntary events handling
on top of [Celery](https://docs.celeryproject.org/).

* AMQP-based robustness of event handling
* Celery tasks interface
* Anti-flood tactics

Installation
------------

```shell script
pip install celery-amqp-events
```

Configuration
-------------

1. Pass a unique "service name" to `Celery()` instance for each service that 
    has event handlers (see `amqp_events.celery:events_app`).
2. Tell celery with `imports` setting where to find event handlers.
3. Configure broker connection and other celery settings.
4. Leave result backend empty - each event may have multiple consumers,
    event result is meaningless in this case.
> You absolutely need to set separate name for each service that consumes 
> events, because without that each fired event will be handled only by
> single randomly chosen service, because your services will share same queue 
> for this event.  

```python
from amqp_events.celery import events_app

app = events_app(
    "service_name",  # important in multi-service environment
    imports=['demo.tasks']  # modules where to find event handlers
    broker_url='amqp://guest:guest@rabbitmq:5672/',
)
```

Adding events and handlers
--------------------------

```python
from demo.celery import app

@app.event('service_name.object_name.event_name')
def even_number_generated(number: int):
    # You may add some validation logic in event function body;
    if number % 2 != 0:
        raise ValueError("number is not even")

@app.handler('service_name.object_name.event_name')
def on_even_number_generated(number: int):
    # Handle event somehow
    print(f"even number {number} generated")

```

Running
-------

* Start ordinary celery worker for your consumer service

> Note that `mingle`, `gossip` and `heartbeat` should be disabled if not used.
> These algorithms use broadcast events, which means that you'll have `N^2` 
> messages in RabbitMQ for `N` workers without any purpose. 

```shell script
celery worker -A your_service.celery \
  --without-mingle --without-heartbeat --without-gossip
```

Sending events
--------------

```python
import random
from demo.events import number_is_even

try:
    number_is_even(random.randint(0, 100))
except ValueError:
    print("oops, number was odd")
```

Robustness
----------

* If event fails with unhandled error, it is retried to separate queue with 
  exponential backoff.
* Backoff is used to prevent resources exhausting (like free http connections)
* If no retry attempts left, unhandled event is moved to "archive" queue
* Archive is used to catch messages which always produce an error in consumer;
  these messages can be manually retried when fix is released.
* Archive is limited both by message TTL and message count limit, so alerts 
  should exist.
* Retry is done via separate queue because of multiple reasons:
    * using `countdown` forces consumer to keep "unacknowledged" events in 
      memory, which is bad for load balancing and resource usage.
    * retrying to same queue will slow down event processing if retry 
      probability is high enougth
    * two faulty consumers retrying same event with **same routing key** will
      cause exponential growth of message count in RabbitMQ because message is 
      split to multiple messages when published from same exchange to multiple 
      queues.
* By default, some fault-tolerance celery settings are enabled:
    * `task_acks_late` will delay task acknowledgement till end of processing
    * `task_reject_on_worker_lost` will prevent `ack` if worker was killed
    * `confirm_publish` in `broker_transport_options` will block producer till 
        broker will confirm that it received incoming message.

Related projects
----------------

### Celery-message-consumer

Robustness tactics is inspired by 
[celery-message-consumer](https://github.com/depop/celery-message-consumer)
project which aims to handle events published to AMQP broker from 
non-celery-based projects (maybe from other languages).
The main difference is that `Celery-AMQP-Events` uses Celery tasks instead of
[including](https://github.com/depop/celery-message-consumer#celery) additional 
`consumer step` nearby the celery worker.
