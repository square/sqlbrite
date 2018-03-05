Change Log
==========

Version 3.2.0 *(2018-03-05)*
----------------------------

 * New: Add `query(SupportSQLiteQuery)` method for one-off queries.


Version 3.1.1 *(2018-02-12)*
----------------------------

 * Fix: Useless `BuildConfig` classes are no longer included.
 * Fix: Eliminate Java interop checks for Kotlin extensions as they're only for Kotlin consumers and the checks exist in the Java code they delegate to anyway.


Version 3.1.0 *(2017-12-18)*
----------------------------

 * New: `inTransaction` Kotlin extension function which handles starting, marking successful, and ending
   a transaction.
 * New: Embedded lint check which validates the number of arguments passed to `query` and `createQuery`
   match the number of expected arguments of the SQL statement.
 * Fix: Properly indent multi-line SQL statements in the logs for `query`.


Version 3.0.0 *(2017-11-28)*
----------------------------

Group ID has changed to `com.squareup.sqlbrite3`.

 * New: Build on top of the Android architecture components Sqlite support library. This allows swapping
   out the underlying Sqlite implementation to that of your choosing.

Because of the way the Sqlite support library works, there is no interop bridge between 1.x or 2.x to
this new version. If you haven't fully migrated to 2.x, complete that migration first and then upgrade
to 3.x all at once.


Version 2.0.0 *(2017-07-07)*
----------------------------

Group ID has changed to `com.squareup.sqlbrite2`.

 * New: RxJava 2.x support. Backpressure is no longer supported as evidenced by the use of
   `Observable`. If you want to slow down query notifications based on backpressure or another metric
   like time then you should apply those operators yourself.
 * New: `mapToOptional` for queries that return 0 or 1 rows.
 * New: `sqlbrite-kotlin` module provides `mapTo*` extension functions for `Observable<Query>`.
 * New: `sqlbrite-interop` module allows bridging 1.x and 2.x libraries together so that notifications
   from each trigger queries from the other.

Note: This version only supports RxJava 2.


Version 1.1.2 *(2017-06-30)*
----------------------------

 * Internal architecture changes to support the upcoming 2.0 release and a bridge allowing both 1.x
   and 2.x to be used at the same time.


Version 1.1.1 *(2016-12-20)*
----------------------------

 * Fix: Correct spelling of `getWritableDatabase()` to match `SQLiteOpenHelper`.


Version 1.1.0 *(2016-12-16)*
----------------------------

 * New: Expose `getReadableDatabase()` and `getWriteableDatabase()` convenience methods.
 * Fix: Do not cache instances of the readable and writable database internally as the framework
   does this by default.


Version 1.0.0 *(2016-12-02)*
----------------------------

 * RxJava dependency updated to 1.2.3.
 * Restore `@WorkerThread` annotations to methods which do I/O. If you're using Java 8 with
   Retrolambda or Jack you need to use version 2.3 or newer of the Android Gradle plugin to have
   these annotations correctly handled by lint.


Version 0.8.0 *(2016-10-21)*
----------------------------

 * New: A `Transformer<Query, Query>` can be supplied which is applied to each returned observable.
 * New: `newNonExclusiveTransaction()` starts transactions in `IMMEDIATE` mode. See the platform
   or SQLite documentation for more information.
 * New: APIs for insert/update/delete which allow providing a compiled `SQLiteStatement`.


Version 0.7.0 *(2016-07-06)*
----------------------------

 * New: Allow `mapTo*` mappers to return `null` values. This is useful when querying on a single,
   nullable column for which `null` is a valid value.
 * Fix: When `mapToOne` does not emit a value downstream, request another value from upstream to
   ensure fixed-item requests (such as `take(1)`) as properly honored.
 * Fix: Add logging to synchronous `execute` methods.


Version 0.6.3 *(2016-04-13)*
----------------------------

 * `QueryObservable` constructor is now public allow instances to be created for tests.


Version 0.6.2 *(2016-03-01)*
----------------------------

 * Fix: Document explicitly and correctly handle the fact that `Query.run()` can return `null` in
   some situations. The `mapToOne`, `mapToOneOrDefault`, `mapToList`, and `asRows` helpers have all
   been updated to handle this case and each is documented with their respective behavior.


Version 0.6.1 *(2016-02-29)*
----------------------------

 * Fix: Apply backpressure strategy between database/content provider and the supplied `Scheduler`.
   This guards against backpressure exceptions when the scheduler is unable to keep up with the rate
   at which queries are being triggered.
 * Fix: Indent the subsequent lines of a multi-line queries when logging.


Version 0.6.0 *(2016-02-17)*
----------------------------

 * New: Require a `Scheduler` when wrapping a database or content provider which will be used when
   sending query triggers. This allows the query to be run in subsequent operators without needing an
   additional `observeOn`. It also eliminates the need to use `subscribeOn` since the supplied
   `Scheduler` will be used for all emissions (similar to RxJava's `timer`, `interval`, etc.).

   This also corrects a potential violation of the RxJava contract and potential source of bugs in that
   all triggers will occur on the supplied `Scheduler`. Previously the initial value would trigger
   synchronously (on the subscribing thread) while subsequent ones trigger on the thread which
   performed the transaction. The new behavior puts the initial trigger on the same thread as all
   subsequent triggers and also does not force transactions to block while sending triggers.


Version 0.5.1 *(2016-02-03)*
----------------------------

 * New: Query logs now contain timing information on how long they took to execute. This only covers
   the time until a `Cursor` was made available, not object mapping or delivering to subscribers.
 * Fix: Switch query logging to happen when `Query.run` is called, not when a query is triggered.
 * Fix: Check for subscribing inside a transaction using a more accurate primitive.


Version 0.5.0 *(2015-12-09)*
----------------------------

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
