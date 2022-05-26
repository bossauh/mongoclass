import random
import unittest
from dataclasses import dataclass

from .. import utils


class TestFind(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        utils.drop_database()

    @classmethod
    def tearDownClass(cls) -> None:
        utils.drop_database()

    def test_find_class(self) -> None:
        client = utils.create_client(engine="mongita_disk")
        User = utils.create_class("user", client)

        # Insert and Get John
        User("John Howard", "john@gmail.com", 8771, "PH").insert()
        john = client.find_class("user", {"name": "John Howard"})
        self.assertTrue(john)
        self.assertTrue(john._mongodb_id)
        self.assertEqual(john.country, "PH")

        unknown = client.find_class("user", {"name": "Some random unknown user"})
        self.assertFalse(unknown)

        # Insert and Get Tony but with a different collection name
        client = utils.create_client()
        User = utils.create_class("user", client, "profile")
        User("Tony Stark", "tonystark@gmail.com", 8080).insert()
        tony = client.find_class("profile", {"phone": 8080})

        self.assertTrue(tony)
        self.assertTrue(tony._mongodb_id)
        self.assertEqual(tony.name, "Tony Stark")

    def test_find_classes(self) -> None:
        client = utils.create_client(engine="mongita_disk")
        Position = utils.create_class("position", client)

        # Create random data
        pos = []
        for _ in range(10):
            d = Position(
                random.randrange(1, 2000),
                random.randrange(1, 2000),
                random.randrange(1, 2000),
            )
            d.insert()
            pos.append(d)

        # Find all of those data
        positions = client.find_classes("position")
        self.assertEqual(positions, pos)

    def test_find_class_different_database(self) -> None:
        client = utils.create_client(engine="mongita_disk")
        Position = utils.create_class(
            "position", client, "coordinates", utils.DATABASES[1]
        )

        pos = Position(1, 2, 3)
        pos.insert()

        # Find it using regular pymongo
        self.assertEqual(
            client.coordinates_data.coordinates.find_one({"x": 1, "y": 2, "z": 3}),
            {"_id": pos._mongodb_id, "x": 1, "y": 2, "z": 3},
        )

        # # Find it using find_class
        result = client.find_class("coordinates", {"x": 1}, database=utils.DATABASES[1])
        self.assertTrue(result)
        self.assertEqual(result, pos)

    def test_find_nested(self) -> None:
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

        metadata = Metadata(NameInformation("Trevor", "Warts"))
        john = User("trevor@gmail.com", metadata)
        insert_result = john.insert()
        self.assertEqual(insert_result.inserted_id, john._mongodb_id)

        # Find it
        query = client.find_class("user", {"email": "trevor@gmail.com"})
        self.assertEqual(query, john)
        john.email = "x"
        self.assertNotEqual(query, john)


if __name__ == "__main__":
    unittest.main()
