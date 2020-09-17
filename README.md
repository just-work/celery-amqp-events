**Celery-AMQP-Events** is a library that implements voluntary events handling
on top of [Celery](https://docs.celeryproject.org/).

* AMQP-based robustness of event handling
* Celery tasks interface
* Anti-flood tactics

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

