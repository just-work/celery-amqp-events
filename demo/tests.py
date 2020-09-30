from unittest import TestCase, mock

from kombu import Queue, Exchange

from amqp_events.events import EventsCelery


class EventsCeleryTestCase(TestCase):
    """ Checks the EventsCelery methods."""

    def setUp(self) -> None:
        super().setUp()
        self.app = EventsCelery("test")
        self.app.conf.broker_url = 'in-memory:///'
        self.app.conf.task_always_eager = True
        self.my_handler_func = mock.MagicMock(__name__='my_handler_func')

    def test_register_handler_function(self):
        """
        A function may be registered as event handler.
        """

        res = self.app.handler("my.event")(self.my_handler_func)

        self.assertIs(self.my_handler_func, res)

        ev = self.app.event("my.event")(lambda: None)

        sentinel = mock.sentinel

        self.app.tasks[ev.name](sentinel)

        self.my_handler_func.assert_called_once_with(sentinel)

    def test_autocreate_queue_for_handler(self):
        """
        If no `task_queues` defined, a queue for each event with handler is
        created.
        """
        self.app.handler("my.event")(self.my_handler_func)

        self.app.on_after_finalize.send(self.app)

        queues = self.app.conf.task_queues
        self.assertEqual(len(queues), 1)
        queue = queues[0]
        self.assertIsInstance(queue, Queue)
        self.assertEqual(queue.name, f"{self.app.main}.my.event")
        self.assertIsInstance(queue.exchange, Exchange)
        self.assertEqual(queue.exchange.name,
                         self.app.conf.task_default_exchange)
        self.assertEqual(queue.exchange.type,
                         self.app.conf.task_default_exchange_type)
        self.assertEqual(queue.routing_key, "my.event")
