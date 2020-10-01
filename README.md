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
    * `Task.autoretry_for` is set to retry on any `Exception` automatically
    * disabling `task_acks_on_failure_or_timeout` forces Celery to reject 
        failed messages if `autoretry_for` failed to handle this.
    * `confirm_publish` in `broker_transport_options` will block producer till 
        broker will confirm that it received incoming message.
        
Delay on broker side
--------------------

Celery`s default retry mechanics is:

* construct a copy of currently handling message
* change arguments or options for the new message from `retry` params
* publish the copy to the same exchange and routing_key found in message 
    `delivery_info`
* acknowledge current message
* after receiving the new message, celery puts it to an in-memory queue to wait 
    until message ETA, and then pass this message to a first free worker.

All this is because `AMQP` protocol doesn't define any delay-related properties.
Currently, `RabbitMQ` has a plugin for delayed messages and some tricky schemes
that allow to delay message delivery on the broker side. We use one of these
schemes.

### Retry causes

A Celery task could be retried in different cases:
* manual retry from task implementation
* retry on an exception listed in `Task.autoretry_for`
* also a task may be rejected if `retry()` failed, if task task failed or if 
    task has timeouted

### Retry routing

To implement broker-side delays `EventsCelery` declares a set of retry exchanges
and queues:
* each exchange and queue is prefixes with "service name"
* each queue is declared with `x-message-ttl` that is equal to `2^n`, where `n` 
    is `AMQP_EVENTS_MAX_RETRIES` env variable and `Task.max_retries` settings
    for every task handler.
* each queue also defines `x-dead-letter-exchange` argument, which points to 
    "recover" exchange
* this "recover" exchange has `topic` type and is bound to same queues as 
    default "events" exchange
* "retry" exchange has type `fanout`, so any message published to this exchange, 
    regardless it's routing key, will be routed to corresponding queue
* with "message ttl" set up, retried message will expire in `2^n` seconds, 
    which is basically exponential backoff implementation
* because of "dead letter exchange", expired message is moved to `recover` 
exchange, from which it is routed again to initial event queue via initial 
    message routing key.

### Reject handling

It's important to retry rejected messages when something goes wrong.

* Each celery task queue is declared with `x-dead-letter-exchange` argument,
    which points to first "retry" exchange.
* As described in "retry routing" section, rejected message is moved by RabbitMQ
    to a retry queue; after a second it expires and is moved again to "recover"
    exchange, and after that to initial event queue.
    
### Archiving

By default, Celery just drops a task when `MaxRetriesExceededError` happens,
but we want to archive such events for some time:

* `EventsCelery` declares a `fanout` "archive" exchange and corresponding queue
* "archive" exchange and queue are also prefixed with "service name"
* "archive" queue is declared with `x-message-ttl` and `x-max-length` arguments,
    and message archive storage is limited both by time and message count.
* When `EventsCelery` finds that task retries count is exceeded, it retries
    this task to "archive" exchange.

### Caveeats

1. Message reordering. What if broker-side delay will shuffle normal message
    ordering? This may break message ETA handling of change order of events.  
    * By default, Celery receives retried and delayed message and keeps it in 
        memory till message ETA. Before ETA it handles other incoming messages, 
        and if these new messages have later ETA, they will be scheduled at
        corresponding time.
    * This is how things work when there are less than `prefetch_multiplier`
        messages in an incoming queue. If there are lot's of incoming messages,
        retried messages will arrive behind ETA time and of cource this changes
        event order.
    * With broker-side delay implementation task `message-ttl` is guaranteed to
        be less or equal to task ETA, so for celery worker the situation will 
        look as a very long amqp `basic.publish` call. If there are no messages,
        celery-side ETA logic will apply and the task will start in time.    
2. Queue count. `RabbitMQ` is sensitive to a total queue count. What if we
    declare too much queues?
    * Total queue count is linear to a max retries count and to events count.
    * Each retry queue is available to any Celery task and implements a delay
        equal to a degree of 2. 20 queues provide max delay more than 23 days,
        but if somebody needs milliseconds delays, he will need 10 more queues.
    * By default each event is routed to it's own queue, but with `topic` 
        exchange you can manually set up a set of queues which handle groups of
        event types. You can even manually setup a single queue for each handled
        event.
    * Worst case needs `N + M + 1` queue, where N is `max_retries` limit and `M`
        is number of handled events.
    * Best case needs `2` queues, if max retries is set to 1 and all events are
        routed to single queue. "Archive" queue may be also disabled. 
3. Celery internals. What if `Celery` will change and all this stuff will break?
    * To trigger auxiluary queues declaration we use celery signals, which we
        consider as public interface.
    * To declare queues we use `Celery.broker_connection()` default channel and
        this could be changed to use separate amqp connection.
    * To bind event queues we extend `Celery.conf.task_queues` with `kombu`
        entities, which provide public interface for queue arguments and 
        additional bindings.
    * To perform retries, we change `Task.retry` arguments, this is also public
        interface.
    * Other things set up to provide more robust events handling are described
        in [Configuration and defaults](https://docs.celeryproject.org/en/stable/userguide/configuration.html)
        section of Celery documentation.
    * At last, we have plans to run integration tests with real celery workers,
        not only unit-tests for mocked celery internals.

### Broker-side Pros

* Observability. There is no way to distinct delayed task and task in a 
    deadlock state except celery events mechanics. RabbitMQ API provides clear
    way to monitor a number of retried and archived messages.
* Lesser memory usage. Default Celery implementation may keep thousands of 
    delayed messages in worker memory. Also it affects AMQP channel `qos`
    parameter, which could not be more that 1000 - this messes things up.
    Broker-side delayed messages are stored on disk in `RabbitMQ` db.  
* Autoretry for failed tasks. By default, each rejected message is lost forever.
    With our setup it will be retried, which is good for delivery guarantees but
    is also bad is message handling breaks constantly (i.e. in case of 
    persistent celery failure). This should be handled carefully. 
* Robustness. Because of asynchronous nature of events handling pattern, we 
    do all efforts to ensure that every single-time fired event is not lost 
    somewhere between network failures and unhandled exceptions in business 
    logic. It's worth mentioning that `RabbitMQ` is considered as durable system
    and we don't touch upon broker failures.

### Cons

* Celery internals. Despite public celery interface usage, we can't guarantee 
    that changes in default celery behavior won't affect broker-side retry model
    as Celery authors decide to move fast and break things in 5.x and later 
    versions.
* Complex logic. Debugging such complex algorithms requires deep RabbitMQ 
    architecture and celery-amqp-events code knowledge. It's not trivial to
    find "the man who can".
* Broker memory usage. Broker-side retry system may require a lot of memory and
    disk space at RabbitMQ, and it's whell-known that RabbitMQ performs much 
    better without any disk usage. Retries should not be used as "normal" event
    handling flow.

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
