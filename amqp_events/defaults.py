from os import getenv as e

# import name for amqp config
AMQP_EVENTS_CONFIG = e('AMQP_EVENTS_CONFIG',
                       'amqp_events.config:AMQP_EVENTS_CONFIG')

ARCHIVED_MESSAGE_TTL = int(e('ARCHIVED_MESSAGE_TTL', 7 * 24 * 3600))
ARCHIVED_QUEUE_LENGTH = int(e('ARCHIVED_QUEUE_LENGTH', 10000))
QUEUE_PREFIX = e('QUEUE_PREFIX', 'events')
