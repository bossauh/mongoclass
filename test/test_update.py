import unittest

from . import utils


class TestUpdate(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        utils.drop_database()

    @classmethod
    def tearDownClass(cls) -> None:
        utils.drop_database()

    def test_update_class(self) -> None:
        client = utils.create_client()
        Position = utils.create_class("position", client)

        # Create a position
        home = Position(72, 63, 12)
        self.assertEqual(home.x, 72)
        insert_result = home.insert()
        self.assertEqual(insert_result.inserted_id, home._mongodb_id)

        # Retrieve the position from the database
        self.assertEqual(client.find_class("position", {"z": 12}), home)

        # Update but don't return new
        insert_result, return_value = home.update({"$set": {"x": 69}}, return_new=False)
        self.assertEqual(insert_result.modified_count, 1)
        self.assertEqual(return_value, home)

        # Update again but return new
        home = client.find_class("position", {"x": 69})
        self.assertTrue(home)
        insert_result, new = home.update({"$set": {"x": 420}})
        self.assertTrue(new)
        self.assertEqual(insert_result.modified_count, 1)
        self.assertNotEqual(new, home)

    def test_update_same_data(self) -> None:
        client = utils.create_client()
        Position = utils.create_class("position", client)

        p1 = Position(1, 2, 3)
        p2 = Position(1, 2, 3)
        p1.insert()
        p2.insert()

        update_result, _ = p1.update({"$set": {"x": 10}})
        self.assertEqual(update_result.modified_count, 1)

        query = client.find_class("position", {"x": 1, "y": 2, "z": 3})
        self.assertTrue(query)
        self.assertEqual(query._mongodb_id, p2._mongodb_id)


if __name__ == "__main__":
    unittest.main()
