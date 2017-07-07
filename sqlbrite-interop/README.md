SQL Brite Interop
=================

In order to allow progressive migration from 1.x to 2.x, you can use this interop bridge to allow
triggers from 1.x to bridge to 2.x and vise-versa.

Create your 1.x and 2.x instances of `SqlBrite`:

```java
com.squareup.sqlbrite.SqlBrite sqlBrite1 = // ...
com.squareup.sqlbrite2.SqlBrite sqlBrite2 = // ...
```

Instead of calling `wrapDatabaseHelper` on each, create a `BriteDatabaseBridge` instead, passing in
the `SQLiteOpenHelper`, the two `SqlBrite` instances, and an RxJava 1.x and 2.x `Scheduler`:

```java
BriteDatabaseBridge bridge = BriteDatabaseBridge.create(
    helper,
    sqlBrite1,
    rx.schedulers.Schedulers.io(),
    sqlBrite2,
    io.reactivex.schedulers.Schedulers.io());
```

Now you can use the `asV1()` and `asV2()` methods to get a `BriteDatabase` for 1.x and 2.x,
respectively.

```java
com.squareup.sqlbrite.BriteDatabase db1 = bridge.asV1();
com.squareup.sqlbrite2.BriteDatabase db2 = bridge.asV2();
```

Any mutative operation on `db1` will trigger queries created from `db2` and vise-versa.



*Note: `BriteContentResolver` does not need a bridge and can be created normally.*
