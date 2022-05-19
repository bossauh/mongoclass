# Mongoclass
A basic ORM like interface for mongodb in python that uses dataclasses.

## Installation
To get started, install mongoclass using pip like so.
```bash
pip install -U mongoclass
```

# Getting Started
This section will explain the basics of how to use mongoclass. After reading this, read the API Reference for more information.
```py
from mongoclass import MongoClassClient
client = MongoClassClient("mongoclass", "localhost:27017")
```
This will create a MongoClassClient instance that exposes the features of mongoclass. MongoClassClient inherits from pymongo.MongoClient so you can also use it like you'd normally use pymongo.

## Schemas
To create a schema (or a preferred term, mongoclass), this is all you have to do.
```py
from dataclasses import dataclass

@client.mongoclass()
@dataclass
class User:
    name: str
    email: str
    phone: int
    country: str = "US"
```
This creates a User mongoclass that belongs in the user collection inside the default database. To create an actual User object and have it be inserted in the database, create an instance of User and call the .insert() method like so
```py
john = User("John Dee", "johndee@gmail.com", 5821)
insert_result = john.insert()
```
The first line creates the user John Dee with the provided information. Notice how we didn't need to provide a country, that is because country defaults to US.

The second line inserts it to the user collection in the default database and then returns a pymongo.InsertOneResult

> As an alternative to having to call .insert(), you can pass _insert=True to User() which will automatically insert as soon as the object is initialized. You do loose the ability to receive the pymongo.InsertOneResult

For the remaining guide and full documentation, click [here](https://oppenheimer.gitbook.io/mongoclass/)

# LICENSE
MIT License

Copyright (c) 2022 Philippe Mathew

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

