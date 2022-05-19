from dataclasses import dataclass

from mongoclass import MongoClassClient
from pymongo import MongoClient

HOST = "localhost:27017"
DATABASES = ["mongoclass", "coordinates_data", "profiles_list"]


def drop_database(database: str = None) -> None:
    # Clear the database using regular pymongo
    client = MongoClient(HOST)

    if database:
        client.drop_database(database or DATABASES[0])
    else:
        for d in DATABASES:
            client.drop_database(d)


def create_client() -> MongoClassClient:
    return MongoClassClient(host=HOST, default_db_name=DATABASES[0])


def create_class(cls: str, client: MongoClassClient, *args, **kwargs):

    if cls == "user":

        @client.mongoclass(*args, **kwargs)
        @dataclass
        class User:
            name: str
            email: str
            phone: int
            country: str = "US"

        return User
    elif cls == "position":

        @client.mongoclass(*args, **kwargs)
        @dataclass
        class Position:
            x: int
            y: int
            z: int

        return Position
