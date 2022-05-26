import copy
import dataclasses
import functools
from typing import List, Optional, Tuple, Union

from mongita import MongitaClientDisk, MongitaClientMemory
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.results import (
    DeleteResult,
    InsertManyResult,
    InsertOneResult,
    UpdateResult,
)


def client_constructor(engine: str, *args, **kwargs):

    if engine == "pymongo":
        engine_class = MongoClient
    elif engine == "mongita_disk":
        engine_class = MongitaClientDisk
    elif engine == "mongita_memory":
        engine_class = MongitaClientMemory
    else:
        raise ValueError(f"Invalid engine '{engine}'")

    class MongoClassClient(engine_class):

        """
        Parameters
        ----------
        `default_db_name` : str
            The name of the default database.
        `*args, **kwargs` :
            To be passed onto `MongoClient()` or `MongitaClientDisk()`
        """

        def __init__(self, default_db_name: str = "main", *args, **kwargs) -> None:
            super().__init__(*args, **kwargs)
            self.mapping = {}
            self.default_database = self[default_db_name]

            # Determine engine being used
            self._engine_used = engine

        def __choose_database(self, database: Union[str, Database] = None) -> Database:
            if database is None:
                return self.default_database
            if isinstance(database, Database):
                return database
            return self[database]

        def map_document(
            self, data: dict, collection: str, database: str, force_nested: bool = False
        ) -> object:
            """
            Map a raw document into a mongoclass.

            Parameters
            ----------
            `data` : dict
                The raw document coming from a collection.
            `collection` : str
                The collection this maps to. The collection then maps onto an actual mongoclass object.
            `database` : str
                The database the raw document belongs to.
            `force_nested` : bool
                Forcefully tell mongoclass that this document is a nested document and it contains other mongoclasses inside it. Defaults to False. Usually this parameter is only set in a recursive manner.
            """

            cls = self.mapping[database][collection]
            if cls["nested"] or force_nested:
                for k, v in copy.copy(data).items():
                    if isinstance(v, dict):
                        if "_nest_collection" in v:
                            data[k] = self.map_document(
                                v["data"],
                                v["_nest_collection"],
                                v["_nest_database"],
                                force_nested=True,
                            )

            _id = data.pop("_id", None)
            if _id:
                data["_mongodb_id"] = _id

            return cls["constructor"](**data)

        def mongoclass(
            self,
            collection: str = None,
            database: Union[str, Database] = None,
            insert_on_init: bool = False,
            nested: bool = False,
        ):

            """
            A decorator used to map a dataclass onto a collection.
            In other words, it converts the dataclass onto a mongoclass.

            Parameters
            ----------
            `collection` : str
                The collection the class must map to. Defaults to the name of the class but lowered.
            `database` : Union[str, Database]
                The database to use. Defaults to the default database.
            `insert_on_init` : bool
                Whether to automatically insert a mongoclass into mongodb whenever a mongoclass instance is created.
                Defaults to False. This is the equivalent of passing `_insert=True` every time you create a mongoclass instance.
                This can also be overwritten by setting `_insert=False`
            `nested` : bool
                Whether this mongoclass has other mongoclasses inside it. Nesting is not automatically determined for performance purposes. Defaults to False.

            """
            db = self.__choose_database(database)

            def wrapper(cls):

                collection_name = collection or cls.__name__.lower()

                @functools.wraps(cls, updated=())
                class Inner(cls):

                    # pylint:disable=no-self-argument
                    def __init__(this, *args, **kwargs) -> None:

                        # MongodDB Attributes
                        this._mongodb_collection = collection_name
                        this._mongodb_db = db
                        this._mongodb_id = kwargs.pop("_mongodb_id", None)

                        _insert = kwargs.pop("_insert", insert_on_init)
                        super().__init__(*args, **kwargs)

                        # Perform inserting if needed
                        if _insert:
                            this.insert()

                    def insert(this, *args, **kwargs) -> InsertOneResult:

                        """
                        Insert this mongoclass as a document in the collection.

                        Parameters
                        ----------
                        `*args, **kwargs` :
                            Other parameters to be passed onto `Database.insert_one`

                        Returns
                        -------
                        `InsertOneResult`
                        """

                        res = this._mongodb_db[this._mongodb_collection].insert_one(
                            this.as_json(), *args, **kwargs
                        )
                        this._mongodb_id = res.inserted_id
                        return res

                    def update(
                        this, operation: dict, *args, **kwargs
                    ) -> Tuple[UpdateResult, object]:

                        """
                        Update this mongoclass document in the collection.

                        Parameters
                        ----------
                        `operation` : dict
                            The operation to be made.
                        `return_new` : bool
                            Whether to return a brand new class containing the updated data. Defaults to False. If this is false, the same object is returned.
                        `*args, **kwargs` :
                            Other parameters to be passed onto `Collection.update_one`

                        Returns
                        -------
                        `Tuple[UpdateResult, Optional[object]]`
                        """

                        return_new = kwargs.pop("return_new", True)

                        res = this._mongodb_db[this._mongodb_collection].update_one(
                            {"_id": this._mongodb_id}, operation, *args, **kwargs
                        )
                        return_value = this
                        if return_new:
                            _id = this._mongodb_id or res.upserted_id
                            if _id:
                                return_value = self.find_class(
                                    this._mongodb_collection,
                                    {"_id": _id},
                                    database=this._mongodb_db,
                                )

                        return (res, return_value)

                    def save(
                        this, *args, **kwargs
                    ) -> Tuple[Union[UpdateResult, InsertOneResult], object]:
                        """
                        Update this mongoclass document in the collection with the current state of the object.

                        If this document doesn't exist yet, it will just call `.insert()`

                        Here's a comparison of `.save()` and `.update()` doing the same exact thing.

                        >>> # Using .update()
                        >>> user.update({"$set": {"name": "Robert Downey"}})
                        >>>
                        >>> # Using .save()
                        >>> user.name = "Rober Downey"
                        >>> user.save()

                        Under the hood, this is just calling .update() using the set operator.

                        Parameters
                        ----------
                        `*args, **kwargs` :
                            To be passed onto `.update()`

                        Returns
                        -------
                        `Tuple[Union[UpdateResult, InsertResult], object]`
                        """

                        if not this._mongodb_id:
                            return (this.insert(), this)

                        data = this.as_json()
                        return this.update({"$set": data}, *args, **kwargs)

                    def delete(this, *args, **kwargs) -> DeleteResult:
                        """
                        Delete this mongoclass in the collection.

                        Parameters
                        ----------
                        `*args, **kwargs` :
                            To be passed onto `Collection.delete_one`

                        Returns
                        -------
                        `DeleteResult`
                        """

                        return this._mongodb_db[this._mongodb_collection].delete_one(
                            {"_id": this._mongodb_id}, *args, **kwargs
                        )

                    def as_json(this, perform_nesting: bool = nested) -> dict:

                        """
                        Convert this mongoclass into a json serializable object. This will pop mongodb and mongoclass reserved attributes such as _mongodb_id, _mongodb_collection, etc.
                        """

                        x = copy.copy(this.__dict__)
                        x.pop("_mongodb_collection", None)
                        x.pop("_mongodb_db", None)
                        x.pop("_mongodb_id", None)
                        x.pop("_id", None)

                        if perform_nesting:
                            for k, v in copy.copy(x).items():
                                if dataclasses.is_dataclass(v):
                                    as_json_method = None
                                    try:
                                        as_json_method = getattr(v, "as_json")
                                    except AttributeError:
                                        pass
                                    if as_json_method:
                                        x[k] = {
                                            "data": as_json_method(perform_nesting),
                                            "_nest_collection": v._mongodb_collection,
                                            "_nest_database": v._mongodb_db.name,
                                        }

                        return x

                if db.name not in self.mapping:
                    self.mapping[db.name] = {}

                self.mapping[db.name][collection_name] = {
                    "constructor": Inner,
                    "nested": nested,
                }
                return Inner

            return wrapper

        def find_class(
            self,
            collection: str,
            *args,
            database: Union[str, Database] = None,
            **kwargs,
        ) -> Optional[object]:

            """
            Find a single document and convert it onto a mongoclass that maps to the collection of the document.

            Parameters
            ----------
            `collection` : str
                The collection to use.
            `*args` :
                Arguments to pass onto `find_one`.
            `database` : Union[str, Database]
                The database to use. Defaults to the default database.
            `**kwargs` :
                Keyword arguments to pass onto `find_one`.

            Returns
            -------
            `Optional[object]` :
                The mongoclass containing the document's data if it exists.
            """

            db = self.__choose_database(database)
            query = db[collection].find_one(*args, **kwargs)
            if not query:
                return
            return self.map_document(query, collection, db.name)

        def find_classes(
            self,
            collection: str,
            *args,
            database: Union[str, Database] = None,
            **kwargs,
        ) -> List[object]:

            """
            Find multiple documents and convert each onto a mongoclass that maps to the collection of the document.

            Parameters
            ----------
            `collection` : str
                The collection to use.
            `*args` :
                Arguments to pass onto `find`.
            `database` : Union[str, Database]
                The database to use. Defaults to the default database.
            `**kwargs` :
                Keyword arguments to pass onto `find`.

            Returns
            -------
            `List[object]` :
                A list of mongoclasses containing the document's data.
            """

            db = self.__choose_database(database)
            query = db[collection].find(*args, **kwargs)
            return [self.map_document(x, collection, db.name) for x in query]

        def insert_classes(
            self, mongoclasses: Union[object, List[object]], *args, **kwargs
        ) -> Union[InsertOneResult, InsertManyResult, List[InsertOneResult]]:
            """
            Insert a mongoclass or a list of mongoclasses into its respective collection and database. This method can accept mongoclasses with different collections and different databases as long as `insert_one` is `True`.

            Notes
            -----
            - If you're inserting multiple mongoclasses with `insert_one=False` and `ordered=False`, the provided mongoclasses will be mutated by setting a `_mongodb_id` attribute with the id coming from `InsertManyResult` after this method executes.

            Parameters
            ----------
            `mongoclasses` : Union[object, List[object]]
                A list of mongoclasses or a single mongoclass. When inserting a single mongoclass, you can just do `mongoclass.insert()`
            `insert_one` : bool
                Whether to call `mongoclass.insert()` on each mongoclass. Defaults to False. False means it would use `Collection.insert_many` to insert all the documents at once.
            `*args, **kwargs` :
                To be passed onto `Collection.insert_many` or `mongoclass.insert()`

            Returns
            -------
            `Union[InsertOneResult, InsertManyResult, List[InsertOneResult]]` :
                - A `InsertOneResult` if the provided `mongoclasses` parameters is just a single mongoclass.
                - A `InsertManyResult` if the provided `mongoclasses` parameter is a list of mongoclasses
            """

            insert_one = kwargs.pop("insert_one", False)
            if not isinstance(mongoclasses, list):
                return mongoclasses.insert(*args, **kwargs)

            if insert_one:
                results = []
                for mongoclass in mongoclasses:
                    results.append(mongoclass.insert(*args, **kwargs))
                return results

            collection, database = (
                mongoclasses[0]._mongodb_collection,
                mongoclasses[0]._mongodb_db,
            )
            insert_result = database[collection].insert_many(
                [x.as_json() for x in mongoclasses], *args, **kwargs
            )
            if kwargs.get("ordered"):
                return insert_result

            for mongoclass, inserted in zip(mongoclasses, insert_result.inserted_ids):
                mongoclass._mongodb_id = inserted

            return insert_result

    return MongoClassClient(*args, **kwargs)


def MongoClassClient(*args, **kwargs):
    return client_constructor("pymongo", *args, **kwargs)
