import unittest
from src.event_handler import EventHandler, parse


class EventHandlerTestCase(unittest.TestCase):
    def setUp(self):
        self.handler = EventHandler()

    def test_parse(self):
        with self.assertRaises(Exception):
            self.assertEquals(parse(''), [''])
            self.assertEquals(parse('test'), ['test'])
        self.assertEquals(parse('666|F|60|50'), [666, 'F', 60, 50])

    def test_invalid_type(self):
        with self.assertRaises(Exception):
            self.handler.process('123|Z')

    def test_follow(self):
        ids = self.handler.process('666|F|60|50')
        self.assertIn(60, self.handler.followers[50])
        self.assertEquals(self.handler.followers[50], {60})
        self.assertEquals([50], ids)

    def test_unfollow(self):
        ids = self.handler.process('1|U|12|9')
        self.assertEquals(len(self.handler.followers[9]), 0)
        self.assertEquals([], ids)

        self.handler.process('666|F|60|50')
        self.assertIn(60, self.handler.followers[50])

        ids = self.handler.process('2|U|60|50')
        self.assertEquals(len(self.handler.followers[50]), 0)
        self.assertEquals([], ids)

    def test_broadcast(self):
        self.assertTrue(self.handler.process('542532|B'))

    def test_private_message(self):
        self.assertEquals([56], self.handler.process('43|P|32|56'))

    def test_status_update(self):
        self.assertEquals([], self.handler.process('634|S|32'))

        self.handler.process('123|F|60|32')
        self.handler.process('124|F|61|32')
        self.assertEquals([60, 61], self.handler.process('634|S|32'))

        self.handler.process('2|U|60|32')
        self.assertEquals([61], self.handler.process('634|S|32'))
