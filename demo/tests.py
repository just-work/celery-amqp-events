import random
from unittest import TestCase, mock

from celery.app.task import Task
from kombu import Queue, Exchange

from amqp_events.celery import EventsCelery


class EventsCeleryTestCase(TestCase):
    """ Checks the EventsCelery methods."""

    def setUp(self) -> None:
        super().setUp()
        self.app = EventsCelery("test")
        self.app.conf.broker_url = 'in-memory:///'
        self.app.conf.task_always_eager = True
        self.my_handler_func = mock.MagicMock(__name__='my_handler_func')
        self.event_name = f'my.event.{random.randint(1, 100000000)}'
        self.my_event = self.app.event(self.event_name)(lambda *a, **kw: None)

    @property
    def task(self):
        return self.app.tasks[self.my_event.name]

    def test_register_handler_function(self):
        """
        A function may be registered as event handler.
        """
        res = self.app.handler(self.event_name)(self.my_handler_func)

        self.assertIs(self.my_handler_func, res)

        sentinel = mock.sentinel

        self.task.apply(args=(sentinel,), throw=True)

        self.my_handler_func.assert_called_once_with(sentinel)

    def test_register_bound_handler_function(self):
        """
        A task argument is passed to a registered event handler with `bind`
        flag set up.
        """
        m = self.my_handler_func

        def bound_handler(self, *args, **kwargs):
            return m(self, *args, **kwargs)

        res = self.app.handler(self.event_name, bind=True)(bound_handler)

        self.assertIs(bound_handler, res)

        sentinel = mock.sentinel

        self.task.apply(args=(sentinel,), throw=True)

        self.my_handler_func.assert_called_once_with(self.task, sentinel)

    def test_register_task_class_as_handler(self):
        """
        An event handler could be a class derived from `EventsCelery.Task`.
        """
        m = self.my_handler_func

        class MyTaskHandler(self.app.Task):
            def run(self, *args, **kwargs):
                return m(self, *args, **kwargs)

        h = self.app.handler(self.event_name)(MyTaskHandler)

        self.assertIs(MyTaskHandler, h)
        self.assertIsInstance(self.task, MyTaskHandler)

        sentinel = mock.sentinel

        self.task.apply(args=(sentinel,), throw=True)

        self.my_handler_func.assert_called_once_with(self.task, sentinel)

    def test_register_any_task_as_handler(self):
        """
        A handle may not inherit from `EventsCelery.Task` class.
        """
        m = self.my_handler_func

        class MyTaskHandler(Task):
            def run(self, *args, **kwargs):
                return m(self, *args, **kwargs)

        h = self.app.handler(self.event_name)(MyTaskHandler)

        self.assertIs(MyTaskHandler, h)
        self.assertIsInstance(self.task, MyTaskHandler)
        self.assertIsInstance(self.task, self.app.Task)

        sentinel = mock.sentinel

        self.task.apply(args=(sentinel,), throw=True)

        self.my_handler_func.assert_called_once_with(self.task, sentinel)

    def test_autocreate_queue_for_handler(self):
        """
        If no `task_queues` defined, a queue for each event with handler is
        created.
        """
        self.app.handler(self.event_name)(self.my_handler_func)

        self.app.on_after_finalize.send(self.app)

        queues = self.app.conf.task_queues
        self.assertEqual(len(queues), 1)
        queue = queues[0]
        self.assertIsInstance(queue, Queue)
        self.assertEqual(queue.name, f"{self.app.main}.{self.event_name}")
        self.assertIsInstance(queue.exchange, Exchange)
        self.assertEqual(queue.exchange.name,
                         self.app.conf.task_default_exchange)
        self.assertEqual(queue.exchange.type,
                         self.app.conf.task_default_exchange_type)
        self.assertEqual(queue.routing_key, self.event_name)

    def test_skip_queue_auto_creation_if_already_defined(self):
        """
        If `task_queues` is set up, no automatic queues are created.
        """
        expected = [Queue("my.queue")]
        self.app.conf.task_queues = expected.copy()
        self.app.handler(self.event_name)(self.my_handler_func)

        self.app.on_after_finalize.send(self.app)

        queues = self.app.conf.task_queues
        self.assertListEqual(expected, queues)

    def test_prevent_double_handler_registration(self):
        """
        Every event has only one event handler.
        """
        decorator = self.app.handler(self.event_name)
        decorator(self.my_handler_func)

        self.assertRaises(RuntimeError, decorator, mock.MagicMock())
