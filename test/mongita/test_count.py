import unittest

from .. import utils


class TestCount(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        utils.drop_database()

    @classmethod
    def tearDownClass(cls) -> None:
        utils.drop_database()

    def test_count_documents(self) -> None:
        client = utils.create_client(engine="mongita_disk")
        Position = utils.create_class("position", client)

        Position(43, 311, 238).save()
        Position(45, 1247, 123).save()

        # Count the number of positions
        self.assertEqual(Position.count_documents({}), 2)
        self.assertEqual(Position.count_documents({"x": 45}), 1)
        self.assertEqual(Position.count_documents({"x": 84}), 0)
