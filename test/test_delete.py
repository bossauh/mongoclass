import unittest

from pymongo.results import DeleteResult

from . import utils


class TestDelete(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        utils.drop_database()

    @classmethod
    def tearDownClass(cls) -> None:
        utils.drop_database()

    def test_mongoclass_delete(self) -> None:
        client = utils.create_client()
        Position = utils.create_class("position", client)

        p1 = Position(50, 20, 30)
        Position(50, 20, 30).insert()
        p1.insert()

        delete_result = p1.delete()
        self.assertIsInstance(delete_result, DeleteResult)
        self.assertEqual(delete_result.deleted_count, 1)

        count = client.default_database.position.count_documents({"x": 50})
        self.assertEqual(count, 1)


if __name__ == "__main__":
    unittest.main()
