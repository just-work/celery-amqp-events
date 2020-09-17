from os import environ

environ.setdefault('AMQP_EVENTS_CONFIG', 'demo.conf:AMQP_EVENTS_CONFIG')

from amqp_events.celery import app  # noqa

__all__ = ['app']
