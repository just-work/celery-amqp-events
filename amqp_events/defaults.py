from os import getenv as e

# Archive ttl in ms
AMQP_EVENTS_ARCHIVED_MESSAGE_TTL = int(e('AMQP_EVENTS_ARCHIVED_MESSAGE_TTL',
                                         7 * 24 * 3600 * 1000))
# Archive queue length
AMQP_EVENTS_ARCHIVED_QUEUE_LENGTH = int(e('AMQP_EVENTS_ARCHIVED_QUEUE_LENGTH',
                                          10000))
# Max number of exponential back-off attempts (2^10 * 2 or ~30 min of attempts)
AMQP_EVENTS_MAX_RETRIES = int(e('AMQP_EVENTS_MAX_RETRIES', 10))
