from typing import Callable, Union

import mongita.cursor
import pymongo.cursor


class Cursor:
    def __init__(
        self,
        cursor: Union[pymongo.cursor.Cursor, mongita.cursor.Cursor],
        mapping_function: Callable,
        collection_name: str,
        database_name: str,
        engine_used: str,
    ) -> None:
        self.internal_cursor = cursor
        self.mapping_function = mapping_function
        self.collection_name = collection_name
        self.database_name = database_name
        self.engine_used = engine_used

    def map_data(self, data: dict):
        return self.mapping_function(data, self.collection_name, self.database_name)

    def __iter__(self):
        for data in self.internal_cursor:
            yield self.map_data(data)

    def __next__(self):
        data = next(self.internal_cursor)
        return self.map_data(data)

    def __getitem__(self, index):
        return self.internal_cursor[index]

    def clone(self):
        return Cursor(
            self.internal_cursor.clone(),
            self.mapping_function,
            self.collection_name,
            self.database_name,
            self.engine_used,
        )

    def close(self):
        self.internal_cursor.close()

    def sort(self, key_or_list, direction=None):
        self.internal_cursor = self.internal_cursor.sort(key_or_list, direction)
        return self

    def limit(self, limit):
        self.internal_cursor = self.internal_cursor.limit(limit)
        return self

    def skip(self, skip):
        self.internal_cursor = self.internal_cursor.skip(skip)
        return self

    def max(self, spec):
        self.internal_cursor = self.internal_cursor.max(spec)
        return self

    def min(self, spec):
        self.internal_cursor = self.internal_cursor.min(spec)
        return self

    def where(self, code):
        self.internal_cursor = self.internal_cursor.where(code)
        return self
