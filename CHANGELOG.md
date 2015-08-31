Change Log
=========

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
