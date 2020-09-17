from celery import Task

from celery.exceptions import MaxRetriesExceededError, Reject


# noinspection PyAbstractClass
class EventHandler(Task):
    max_retries = 5

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        kw = dict(
            args=args,
            kwargs=kwargs,
            throw=False,
            countdown=0,
        )
        try:
            self.retry(routing_key=f'{self.name}.retry',
                       expiration=2 ** self.request.retries, **kw)
        except MaxRetriesExceededError:
            self.request.retries = 0
            self.retry(routing_key=f'{self.name}.archived', exc=exc, **kw)

    def retry(self, args=None, kwargs=None, exc=None, throw=True, eta=None,
              countdown=None, max_retries=None, **options):
        # handle task send errors on retry
        try:
            return super().retry(args, kwargs, exc, throw, eta, countdown,
                                 max_retries, **options)
        except MaxRetriesExceededError:
            raise
        except Exception as e:
            if exc is not None:
                # instead of MaxRetriesExceededError celery raises existing
                # error, so it is not connection failure, skip reject
                raise
            raise Reject(reason=e, requeue=True)
