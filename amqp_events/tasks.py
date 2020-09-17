from celery import Task

from celery.exceptions import Reject

from amqp_events import defaults


class EventHandler(Task):
    max_retries = defaults.AMQP_EVENTS_MAX_RETRIES

    def run(self, *args, **kwargs):
        raise NotImplementedError

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        # retry task several times through separate queue, then archive message
        # when no attempts left
        kw = dict(
            args=args,
            kwargs=kwargs,
            throw=False,
            countdown=0,
        )
        if self.request.retries >= self.max_retries:
            self.request.retries -= 1
            routing_key = f'{self.name}.archived'
            expiration = None
        else:
            routing_key = f'{self.name}.retry'
            expiration = 2 ** self.request.retries
        self.retry(routing_key=routing_key,
                   exc=exc,
                   expiration=expiration,
                   **kw)

    def retry(self, args=None, kwargs=None, exc=None, throw=True, eta=None,
              countdown=None, max_retries=None, **options):
        # handle task send errors on retry
        try:
            return super().retry(args, kwargs, exc, throw, eta, countdown,
                                 max_retries, **options)
        except Exception as e:
            raise Reject(reason=e, requeue=True)
