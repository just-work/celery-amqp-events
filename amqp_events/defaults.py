from os import getenv as e

# import name for amqp config
AMQP_EVENTS_CONFIG = e('AMQP_EVENTS_CONFIG',
                       'amqp_events.config:AMQP_EVENTS_CONFIG')

# archive ttl in ms
AMQP_EVENTS_ARCHIVED_MESSAGE_TTL = int(e('AMQP_EVENTS_ARCHIVED_MESSAGE_TTL',
                                         7 * 24 * 3600 * 1000))
# archive queue length
AMQP_EVENTS_ARCHIVED_QUEUE_LENGTH = int(e('AMQP_EVENTS_ARCHIVED_QUEUE_LENGTH',
                                          10000))
AMQP_EVENTS_QUEUE_PREFIX = e('AMQP_EVENTS_QUEUE_PREFIX', 'events')
AMQP_EVENTS_MAX_RETRIES = int(e('AMQP_EVENTS_MAX_RETRIES', 10))
