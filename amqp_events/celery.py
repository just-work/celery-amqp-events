from typing import Any

from amqp_events import config, events


def events_app(main: str, **kwargs: Any) -> events.EventsCelery:
    """
    Initializes new EventsCelery application
    :param main: application name used as queue prefix
    :param kwargs: additional celery configuration
    :return: applcation instance
    """
    app = events.EventsCelery(main)
    app.config_from_object(config.AMQP_EVENTS_CONFIG)
    app.conf.update(kwargs)
    return app
