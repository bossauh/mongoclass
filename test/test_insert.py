import unittest

from pymongo.results import InsertManyResult, InsertOneResult

from . import utils


class TestInsert(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        utils.drop_database()

    @classmethod
    def tearDownClass(cls) -> None:
        utils.drop_database()

    def test_insert(self) -> None:
        client = utils.create_client()

        User = utils.create_class("user", client)

        # Insert John
        john = User("John Howard", "john@gmail.com", 8771, "PH")
        self.assertEqual(john.country, "PH")
        self.assertFalse(john._mongodb_id)
        insert_res = john.insert()
        self.assertIsInstance(insert_res, InsertOneResult)
        self.assertEqual(john._mongodb_id, insert_res.inserted_id)

        # Insert Kenneth
        kenneth = User("Kenneth Richards", "kennethrichards@gmail.com", 7421)
        self.assertEqual(kenneth.country, "US")
        self.assertFalse(kenneth._mongodb_id)
        insert_res = kenneth.insert()
        self.assertIsInstance(insert_res, InsertOneResult)
        self.assertEqual(kenneth._mongodb_id, insert_res.inserted_id)

        # Insert Tony
        tony = User("Tony Stark", "tonystark@gmail.com", 8080)
        self.assertEqual(tony.country, "US")
        self.assertFalse(tony._mongodb_id)
        insert_res = tony.insert()
        self.assertIsInstance(insert_res, InsertOneResult)
        self.assertEqual(tony._mongodb_id, insert_res.inserted_id)

        # Make sure the _id and other stuff are not in the as_json
        as_json = tony.as_json()
        self.assertDictEqual(
            as_json,
            {
                "name": "Tony Stark",
                "email": "tonystark@gmail.com",
                "phone": 8080,
                "country": "US",
            },
        )

        # Insert in a different database and a different collection name
        Profile = utils.create_class("user", client, "profile", utils.DATABASES[2])
        joe = Profile("Joe Dart", "joedart@gmail.com", 1500)
        insert_result = joe.insert()
        self.assertEqual(insert_result.inserted_id, joe._mongodb_id)

    def test_auto_insert(self) -> None:
        client = utils.create_client()
        Position = utils.create_class("position", client)

        self.assertTrue(Position(1, 2, 3, _insert=True)._mongodb_id)
        self.assertTrue(client.find_class("position", {"x": 1}))

    def test_insert_classes(self) -> None:
        client = utils.create_client()
        Position = utils.create_class("position", client)

        # Insert One
        pos = Position(1, 2, 3)
        insert_result = client.insert_classes(pos)
        self.assertIsInstance(insert_result, InsertOneResult)
        self.assertEqual(insert_result.inserted_id, pos._mongodb_id)

        # Insert many but with insert_one=True
        pos = [Position(1, 2, 3), Position(4, 5, 6)]
        insert_result = client.insert_classes(pos, insert_one=True)
        self.assertIsInstance(insert_result, list)
        for x, y in zip(pos, insert_result):
            self.assertEqual(x._mongodb_id, y.inserted_id)

        # Insert many but with insert_one=False
        pos = [Position(1, 2, 3), Position(4, 5, 6)]
        insert_result = client.insert_classes(pos)
        self.assertIsInstance(insert_result, InsertManyResult)
        for x, y in zip(pos, insert_result.inserted_ids):
            self.assertEqual(x._mongodb_id, y)

    def test_insert_same_data(self) -> None:
        client = utils.create_client()
        Position = utils.create_class("position", client)

        p1 = Position(1, 2, 3)
        p2 = Position(1, 2, 3)
        i1 = p1.insert()
        i2 = p2.insert()

        self.assertNotEqual(i1.inserted_id, i2.inserted_id)

    def test_insert_auto_by_default(self) -> None:
        client = utils.create_client()
        Position = utils.create_class("position", client, insert_on_init=True)

        p1 = Position(1, 2, 3)
        p2 = Position(4, 5, 6)

        self.assertEqual(client.find_class("position", {"x": 1}), p1)
        self.assertEqual(client.find_class("position", {"x": 4}), p2)
        self.assertFalse(client.find_class("position", {"x": 2}))


if __name__ == "__main__":
    unittest.main()
