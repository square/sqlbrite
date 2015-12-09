Change Log
=========

Version 0.5.0 *(In Development)*
--------------------------------

 * New: Expose `mapToOne`, `mapToOneOrDefault`, and `mapToList` as static methods on `Query`. These
   mirror the behavior of the methods of the same name on `QueryObservable` but can be used later in
   a stream by passing the returned `Operator` instances to `lift()` (e.g.,
   `take(1).lift(Query.mapToOne(..))`).
 * Requires RxJava 1.1.0 or newer.


Version 0.4.1 *(2015-10-19)*
----------------------------

 * New: `execute` method provides the ability to execute arbitrary SQL statements.
 * New: `executeAndTrigger` method provides the ability to execute arbitrary SQL statements and
   notifying any queries to update on the specified table.
 * Fix: `Query.asRows` no longer calls `onCompleted` when the downstream subscriber has unsubscribed.


Version 0.4.0 *(2015-09-22)*
----------------------------

 * New: `mapToOneOrDefault` replaces `mapToOneOrNull` for more flexibility.
 * Fix: Notifications of table updates as the result of a transaction now occur after the transaction
   has been applied. Previous the notification would happen during the commit at which time it was
   invalid to create a new transaction in a subscriber.


Version 0.3.1 *(2015-09-02)*
----------------------------

 * New: `mapToOne` and `mapToOneOrNull` operators on `QueryObservable`. These work on queries which
   return 0 or 1 rows and are a convenience for turning them into a type `T` given a mapper of type
   `Func1<Cursor, T>` (the same which can be used for `mapToList`).
 * Fix: Remove `@WorkerThread` annotations for now. Various combinations of lint, RxJava, and
   retrolambda can cause false-positives.


Version 0.3.0 *(2015-08-31)*
----------------------------

 * Transactions are now exposed as objects instead of methods. Call `newTransaction()` to start a
   transaction. On the `Transaction` instance, call `markSuccessful()` to indicate success and
   `end()` to commit or rollback the transaction. The `Transaction` instance implements `Closeable`
   to allow its use in a try-with-resources construct. See the `newTransaction()` Javadoc for more
   information.
 * `Query` instances can now be turned directly into an `Observable<T>` by calling `asRows` with a
   `Func1<Cursor, T>` that maps rows to a type `T`. This allows easy filtering and limiting in
   memory rather than in the query. See the `asRows` Javadoc for more information.
 * `createQuery` now returns a `QueryObservable` which offers a `mapToList` operator. This operator
   also takes a `Func1<Cursor, T>` for mapping rows to a type `T`, but instead of individual rows it
   collects all the rows into a list. For large query results or frequently updated tables this can
   create a lot of objects. See the `mapToList` Javadoc for more information.
 * New: Nullability, `@CheckResult`, and `@WorkerThread` annotations on all APIs allow a more useful
   interaction with lint in consuming projects.


Version 0.2.1 *(2015-07-14)*
----------------------------

 * Fix: Add support for backpressure.


Version 0.2.0 *(2015-06-30)*
----------------------------

 * An `Observable<Query>` can now be created from wrapping a `ContentResolver` in order to observe
   queries from another app's content provider.
 * `SqlBrite` class is now a factory for both a `BriteDatabase` (the `SQLiteOpenHelper` wrapper)
   and `BriteContentResolver` (the `ContentResolver` wrapper).


Version 0.1.0 *(2015-02-21)*
----------------------------

Initial release.
