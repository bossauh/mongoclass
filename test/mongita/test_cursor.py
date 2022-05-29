import random
import unittest

from mongoclass.cursor import Cursor

from .. import utils

ENGINE = "mongita_disk"


class TestCursor(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        utils.drop_database()

    @classmethod
    def tearDownClass(cls) -> None:
        utils.drop_database()

    def test_cursor(self) -> None:
        client = utils.create_client(ENGINE)
        Position = utils.create_class("position", client)
        local_positions = [
            Position(200, 300, 400),
            Position(600, 800, 700),
            Position(55, 30, 60),
        ]

        client.insert_classes(local_positions)

        positions = client.find_classes("position")
        self.assertIsInstance(positions, Cursor)

        positions_as_list = []
        for inserted, local in zip(positions, local_positions):
            self.assertEqual(inserted, local)
            positions_as_list.append(inserted)

        positions = positions.clone()
        self.assertEqual(positions_as_list, list(positions))
        self.assertEqual(positions_as_list, local_positions)

        positions = positions.clone()
        iterator = iter(positions)
        self.assertEqual(next(iterator), local_positions[0])
        self.assertEqual(next(iterator), local_positions[1])
        self.assertEqual(next(iterator), local_positions[2])

    def test_cursor_methods(self) -> None:
        client = utils.create_client(ENGINE)
        Position = utils.create_class("position", client, "coordinates")

        positions = []
        for _ in range(20):
            positions.append(
                Position(
                    x=random.randint(1000, 65536),
                    y=random.randint(1000, 65536),
                    z=random.randint(1000, 65536),
                )
            )

        client.insert_classes(positions)

        # Sort
        local_sorted = sorted(positions, key=lambda x: x.x)
        db_sorted = list(client.find_classes("coordinates").sort("x", 1))
        self.assertEqual(local_sorted, db_sorted)

        # Limit
        db_limited = list(client.find_classes("coordinates").limit(3))
        self.assertEqual(db_limited, positions[:3])

        # Skip
        db_skipped = list(client.find_classes("coordinates").skip(3))
        self.assertEqual(db_skipped, positions[3:])


if __name__ == "__main__":
    unittest.main()
