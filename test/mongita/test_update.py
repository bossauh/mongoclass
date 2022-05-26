import unittest
from dataclasses import dataclass
from typing import List

from .. import utils


class TestUpdate(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        utils.drop_database()

    @classmethod
    def tearDownClass(cls) -> None:
        utils.drop_database()

    def test_update_class(self) -> None:
        client = utils.create_client(engine="mongita_disk")
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
        client = utils.create_client(engine="mongita_disk")
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

    def test_update_save(self) -> None:
        client = utils.create_client(engine="mongita_disk")

        @client.mongoclass()
        @dataclass
        class User:
            name: str
            age: int
            skills: List[str]
            country: str = "US"

        john = User("John Howard", 21, ["programming"])
        john.insert()

        john_find = client.find_class("user", {"name": "John Howard"})
        self.assertEqual(john_find, john)

        # Update via save
        john_find.age += 22
        john_find.country = "UK"
        john_find.skills.append("designing")

        # Check and make sure object attributes actually updated locally that is
        self.assertEqual(john_find.skills, ["programming", "designing"])

        # Check and make sure it has not yet updated
        self.assertEqual(client.find_class("user", {"name": "John Howard"}), john)

        # Finally update and check if it did actually update
        update_result, new_john = john_find.save(return_new=True)
        self.assertEqual(update_result.modified_count, 1)
        self.assertEqual(new_john, john_find)
        self.assertNotEqual(new_john, john)

    def test_update_nested(self) -> None:
        client = utils.create_client(engine="mongita_disk")

        @client.mongoclass()
        @dataclass
        class NameInformation:
            first: str
            last: str

        @client.mongoclass()
        @dataclass
        class Metadata:
            name: NameInformation

        @client.mongoclass(nested=True)
        @dataclass
        class User:
            email: str
            metadata: Metadata

        scott = User("scott@gmail.com", Metadata(NameInformation("Scott", "Jones")))
        scott.insert()
        scott.metadata.name.last = "Dart"
        update_result, new_object = scott.save()
        self.assertEqual(update_result.modified_count, 1)
        self.assertEqual(new_object.metadata.name.last, "Dart")


if __name__ == "__main__":
    unittest.main()
