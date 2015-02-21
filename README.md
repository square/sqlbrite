SQLBrite
========

A lightweight wrapper around `SQLiteOpenHelper` which introduces reactive stream semantics to SQL
operations.



Usage
-----

Wrap a `SQLiteOpenHelper` instance with `SqlBrite`:

```java
SqlBrite db = SqlBrite.create(helper);
```

The `SqlBrite.createQuery` method is similar to `SQLiteOpenHelper.rawQuery` except it takes an
additional parameter of table(s) on which to listen for changes. Subscribe to the returned
`Observable<Query>` which will immediately notify with a `Query` to run.

```java
Observable<Query> users = db.createQuery("users", "SELECT * FROM users");
users.subscribe(new Action1<Query>() {
  @Override public void call(Query query) {
    Cursor cursor = query.run();
    // TODO parse data...
  }
});
```

Unlike a traditional `rawQuery`, updates to the specified table(s) will trigger additional
notifications for as long as you remain subscribed to the observable. This means that when you
insert, update, or delete data, any subscribed queries will update with the new data instantly.

```java
final AtomicInteger queries = new AtomicInteger();
users.subscribe(new Action1<Query>() {
  @Override public void call(Query query) {
    queries.getAndIncrement();
  }
});
System.out.println("Queries: " + queries.get()); // Prints 1

db.insert("users", createUser("jw", "Jake Wharton"));
db.insert("users", createUser("mattp", "Matt Precious"));
db.insert("users", createUser("strong", "Alec Strong"));

System.out.println("Queries: " + queries.get()); // Prints 4
```

In the previous example we re-used the `SqlBrite` object "db" for inserts. All insert, update, or
delete operations must go through this object in order to correctly notify subscribers.

Unsubscribe from the returned `Subscription` to stop getting updates.

```java
final AtomicInteger queries = new AtomicInteger();
Subscription s = users.subscribe(new Action1<Query>() {
  @Override public void call(Query query) {
    queries.getAndIncrement();
  }
});
System.out.println("Queries: " + queries.get()); // Prints 1

db.insert("users", createUser("jw", "Jake Wharton"));
db.insert("users", createUser("mattp", "Matt Precious"));
s.unsubscribe();

db.insert("users", createUser("strong", "Alec Strong"));

System.out.println("Queries: " + queries.get()); // Prints 3
```

Use transactions to prevent large changes to the data from spamming your subscribers.

```java
final AtomicInteger queries = new AtomicInteger();
users.subscribe(new Action1<Query>() {
  @Override public void call(Query query) {
    queries.getAndIncrement();
  }
});
System.out.println("Queries: " + queries.get()); // Prints 1

db.beginTransaction();
try {
  db.insert("users", createUser("jw", "Jake Wharton"));
  db.insert("users", createUser("mattp", "Matt Precious"));
  db.insert("users", createUser("strong", "Alec Strong"));
  db.setTransactionSuccessful();
} finally {
  db.endTransaction();
}

System.out.println("Queries: " + queries.get()); // Prints 2
```

Since queries are just regular RxJava `Observable` objects, operators can also be used to
control the frequency of notifications to subscribers.

```java
users.debounce(500, MILLISECONDS).subscribe(new Action1<Query>() {
  @Override public void call(Query query) {
    // TODO...
  }
});
```

The full power of RxJava's operators are available for combining, filtering, and triggering any
number of queries and data changes.



Philosophy
----------

SqlBrite's only responsibility is to be a mechanism for coordinating and composing the notification
of updates to tables such that you can update queries as soon as data changes.

This library is not an ORM. It is not a type-safe query mechanism. It won't serialize the same POJOs
you use for Gson. It's not going to perform database migrations for you.

A day may come when some of those features are added, but it is not this day.



Download
--------

```groovy
compile 'com.squareup.sqlbrite:sqlbrite:0.1.0-SNAPSHOT'
```

Snapshots of the development version are available in [Sonatype's `snapshots` repository][snap].



License
-------

    Copyright 2015 Square, Inc.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.





 [snap]: https://oss.sonatype.org/content/repositories/snapshots/
