import unittest
import queue
from src.source_server import SourceServer

class SourceServerTestCase(unittest.TestCase):
    def setUp(self):
        self.queue = queue.Queue()
        self.server = SourceServer(self.queue, 10)

    def test_buffer_idx(self):
        self.assertEqual(0, self.server.buffer_idx(0))
        self.assertEqual(9, self.server.buffer_idx(9))
        self.assertEqual(0, self.server.buffer_idx(10))
        self.assertEqual(1, self.server.buffer_idx(11))

    def test_receive(self):
        msg = '11|S|32'
        seq, idx = self.server.receive(msg)
        self.assertEqual(11, seq)
        self.assertEqual(1, idx)
        self.assertEqual((11, msg), self.server.buffer[1])

        with self.assertRaises(BufferError):
            self.server.receive(msg)
        
    def test_should_process(self):
        self.server.receive('3|S|32')
        self.assertFalse(self.server.should_process(2))
        self.assertTrue(self.server.should_process(1))

    def test_collect_sequence(self):
        self.assertEqual(0, self.server.last_seq)
        self.assertEqual([], self.server.collect_sequence(0))

        msg1 = '1|S|32'
        msg2 = '2|B'
        msg3 = '3|B'
        msg5 = '5|U|123|666'
        self.server.receive(msg1)
        self.server.receive(msg2)
        self.server.receive(msg3)
        self.server.receive(msg5)

        self.assertEqual([], self.server.collect_sequence(0))
        self.assertEqual([msg1, msg2, msg3], self.server.collect_sequence(1))
        self.assertEqual(3, self.server.last_seq)

        self.assertEqual([msg5], self.server.collect_sequence(5))
        self.assertEqual(5, self.server.last_seq)

    def test_buffer_messages(self):
        self.server.last_seq = 666
        msg667 = '667|F|12|13'
        msg668 = '668|B'
        msg669 = '669|B'
        msg670 = '670|S|13'
        msg671 = '671|U|12|13'

        self.server.buffer_messages([msg670, msg671, msg669])
        self.assertTrue(self.server.queue.empty())
        self.assertEqual(666, self.server.last_seq)

        self.server.buffer_messages([msg667, msg668])
        self.assertEqual(671, self.server.last_seq)
        self.assertEqual(5, self.server.queue.qsize())    
        