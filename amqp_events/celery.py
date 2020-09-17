from celery.app import Celery
from celery.app.utils import Settings
from celery.apps.worker import Worker
from celery.signals import celeryd_after_setup, celeryd_init
from kombu import Queue, Exchange
from kombu.utils import symbol_by_name

from amqp_events import defaults, config

cfg = symbol_by_name(defaults.AMQP_EVENTS_CONFIG)
celery = Celery()

celery.config_from_object(config.AMQP_EVENTS_CONFIG)
celery.conf.update(cfg)


@celeryd_init.connect
def on_celeryd_init(*, instance: Worker, conf: Settings, **_):
    queues = conf.task_queues
    if not queues:
        exchange = Exchange(
            name=conf.task_default_exchange,
            type=conf.task_default_exchange_type)
        queue = Queue(
            name=conf.task_default_queue,
            exchange=exchange,
            routing_key=conf.task_default_routing_key)
        queues.append(queue)
    config.initialize_task_queues(instance.app, queues)
