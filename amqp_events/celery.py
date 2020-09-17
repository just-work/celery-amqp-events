from typing import Any

from amqp_events import config, events


def events_app(main: str, **cfg: Any) -> events.EventsCelery:
    """
    Initializes new EventsCelery application
    :param main: application name used as queue prefix
    :param cfg: additional celery configuration
    :return: applcation instance
    """
    app = events.EventsCelery(main)
    app.config_from_object(config.AMQP_EVENTS_CONFIG)
    app.config_from_object(cfg)
    return app
