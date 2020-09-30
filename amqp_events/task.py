from datetime import datetime
from typing import Any, Tuple, Dict, Optional, Union

from billiard.einfo import ExceptionInfo
from celery import Task

from amqp_events import defaults

Args = Tuple[Any, ...]
Kwargs = Dict[str, Any]


class EventHandler(Task):
    max_retries = defaults.AMQP_EVENTS_MAX_RETRIES

    # As retry queue handles message delay by itself, we don't need default
    # retry countdown.
    default_retry_delay = 0

    def run(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError

    def on_failure(self, exc: Exception, task_id: str, args: Args,
                   kwargs: Kwargs, einfo: ExceptionInfo) -> None:
        self.retry(exc=exc, throw=False)

    def retry(self,
              args: Optional[Args] = None,
              kwargs: Optional[Kwargs] = None,
              exc: Optional[Exception] = None,
              throw: bool = True,
              eta: Optional[datetime] = None,
              countdown: Union[None, int, float] = None,
              max_retries: Optional[int] = None,
              **options: Any) -> None:
        max_retries = self.max_retries if max_retries is None else max_retries

        if max_retries is not None and self.request.retries >= max_retries:
            # When no attempts left we retry message to separate archive queue
            # where it can be found for some time before expiration.
            options['exchange'] = f'{self.app.main}.archived'

            # One more attempt to move message to archive queue
            max_retries += 1

        else:
            # As celery uses delivery_info for retrying tasks, this can lead to
            # exponential event count growth when two different consumers retry
            # same task with same shared routing key.
            # Because of this we always retry message to separate retry queue
            # via separate fanout exchange with vadying message-ttl
            if self.request.retries == 0:
                name = f'{self.app.main}.retry'
            else:
                name = f'{self.app.main}.retry.{self.request.retries}'
            options['exchange'] = name

        return super().retry(args, kwargs, exc, throw, eta, countdown,
                             max_retries, **options)
