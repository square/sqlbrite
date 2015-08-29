/*
 * Copyright (C) 2015 Square, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.squareup.sqlbrite;

import android.content.ContentValues;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteTransactionListener;
import android.support.annotation.CheckResult;
import android.support.annotation.IntDef;
import android.support.annotation.NonNull;
import android.support.annotation.Nullable;
import android.support.annotation.WorkerThread;
import com.squareup.sqlbrite.SqlBrite.Query;
import java.io.Closeable;
import java.io.IOException;
import java.lang.annotation.Retention;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import rx.Observable;
import rx.functions.Func1;
import rx.subjects.PublishSubject;

import static android.database.sqlite.SQLiteDatabase.CONFLICT_ABORT;
import static android.database.sqlite.SQLiteDatabase.CONFLICT_FAIL;
import static android.database.sqlite.SQLiteDatabase.CONFLICT_IGNORE;
import static android.database.sqlite.SQLiteDatabase.CONFLICT_NONE;
import static android.database.sqlite.SQLiteDatabase.CONFLICT_REPLACE;
import static android.database.sqlite.SQLiteDatabase.CONFLICT_ROLLBACK;
import static java.lang.annotation.RetentionPolicy.SOURCE;

/**
 * A lightweight wrapper around {@link SQLiteOpenHelper} which allows for continuously observing
 * the result of a query. Create using a {@link SqlBrite} instance.
 */
public final class BriteDatabase implements Closeable {
  private static final Set<String> INITIAL_TRIGGER = Collections.singleton("<initial>");

  private final SQLiteOpenHelper helper;
  private final SqlBrite.Logger logger;

  // Package-private to avoid synthetic accessor method for 'transaction' instance.
  final ThreadLocal<SqliteTransaction> transactions = new ThreadLocal<>();
  /** Publishes sets of tables which have changed. */
  private final PublishSubject<Set<String>> triggers = PublishSubject.create();

  private final Transaction transaction = new Transaction() {
    @Override public void markSuccessful() {
      if (logging) log("TXN SUCCESS %s", transactions.get());
      getWriteableDatabase().setTransactionSuccessful();
    }

    @Override public boolean yieldIfContendedSafely() {
      return getWriteableDatabase().yieldIfContendedSafely();
    }

    @Override public boolean yieldIfContendedSafely(long sleepAmount, TimeUnit sleepUnit) {
      return getWriteableDatabase().yieldIfContendedSafely(sleepUnit.toMillis(sleepAmount));
    }

    @Override public void end() {
      close();
    }

    @Override public void close() {
      SqliteTransaction transaction = transactions.get();
      if (transaction == null) {
        throw new IllegalStateException("Not in transaction.");
      }
      SqliteTransaction newTransaction = transaction.parent;
      transactions.set(newTransaction);
      if (logging) log("TXN END %s", transaction);
      getWriteableDatabase().endTransaction();
    }
  };

  // Read and write guarded by 'databaseLock'. Lazily initialized. Use methods to access.
  private volatile SQLiteDatabase readableDatabase;
  private volatile SQLiteDatabase writeableDatabase;
  private final Object databaseLock = new Object();

  // Package-private to avoid synthetic accessor method for 'transaction' instance.
  volatile boolean logging;

  BriteDatabase(@NonNull SQLiteOpenHelper helper, @NonNull SqlBrite.Logger logger) {
    this.helper = helper;
    this.logger = logger;
  }

  /**
   * Control whether debug logging is enabled.
   */
  public void setLoggingEnabled(boolean enabled) {
    logging = enabled;
  }

  private SQLiteDatabase getReadableDatabase() {
    SQLiteDatabase db = readableDatabase;
    if (db == null) {
      synchronized (databaseLock) {
        db = readableDatabase;
        if (db == null) {
          if (logging) log("Creating readable database");
          db = readableDatabase = helper.getReadableDatabase();
        }
      }
    }
    return db;
  }

  // Package-private to avoid synthetic accessor method for 'transaction' instance.
  SQLiteDatabase getWriteableDatabase() {
    SQLiteDatabase db = writeableDatabase;
    if (db == null) {
      synchronized (databaseLock) {
        db = writeableDatabase;
        if (db == null) {
          if (logging) log("Creating writeable database");
          db = writeableDatabase = helper.getWritableDatabase();
        }
      }
    }
    return db;
  }

  private void sendTableTrigger(Set<String> tables) {
    SqliteTransaction transaction = transactions.get();
    if (transaction != null) {
      transaction.triggers.addAll(tables);
    } else {
      if (logging) log("TRIGGER %s", tables);
      triggers.onNext(tables);
    }
  }

  /**
   * Begin a transaction for this thread.
   * <p>
   * Transactions may nest. If the transaction is not in progress, then a database connection is
   * obtained and a new transaction is started. Otherwise, a nested transaction is started.
   * <p>
   * Each call to {@code newTransaction} must be matched exactly by a call to
   * {@link Transaction#end()}. To mark a transaction as successful, call
   * {@link Transaction#markSuccessful()} before calling {@link Transaction#end()}. If the
   * transaction is not successful, or if any of its nested transactions were not successful, then
   * the entire transaction will be rolled back when the outermost transaction is ended.
   * <p>
   * Transactions queue up all query notifications until they have been applied.
   * <p>
   * Here is the standard idiom for transactions:
   *
   * <pre>{@code
   * try (Transaction transaction = db.newTransaction()) {
   *   ...
   *   transaction.markSuccessful();
   * }
   * }</pre>
   *
   * Manually call {@link Transaction#end()} when try-with-resources is not available:
   * <pre>{@code
   * Transaction transaction = db.newTransaction();
   * try {
   *   ...
   *   transaction.markSuccessful();
   * } finally {
   *   transaction.end();
   * }
   * }</pre>
   *
   *
   * @see SQLiteDatabase#beginTransaction()
   */
  public Transaction newTransaction() {
    SqliteTransaction transaction = new SqliteTransaction(transactions.get());
    transactions.set(transaction);
    if (logging) log("TXN BEGIN %s", transaction);
    getWriteableDatabase().beginTransactionWithListener(transaction);

    return this.transaction;
  }

  /**
   * Close the underlying {@link SQLiteOpenHelper} and remove cached readable and writeable
   * databases. This does not prevent existing observables from retaining existing references as
   * well as attempting to create new ones for new subscriptions.
   */
  @Override public void close() throws IOException {
    synchronized (databaseLock) {
      readableDatabase = null;
      writeableDatabase = null;
      helper.close();
    }
  }

  /**
   * Create an observable which will notify subscribers with a {@linkplain Query query} for
   * execution. Subscribers are responsible for <b>always</b> closing {@link Cursor} instance
   * returned from the {@link Query}.
   * <p>
   * Subscribers will receive an immediate notification for initial data as well as subsequent
   * notifications for when the supplied {@code table}'s data changes through the {@code insert},
   * {@code update}, and {@code delete} methods of this class. Unsubscribe when you no longer want
   * updates to a query.
   * <p>
   * <b>Warning:</b> this method does not perform the query! Only by subscribing to the returned
   * {@link Observable} will the operation occur.
   *
   * @see SQLiteDatabase#rawQuery(String, String[])
   */
  @CheckResult
  public Observable<Query> createQuery(@NonNull final String table, @NonNull String sql,
      @NonNull String... args) {
    Func1<Set<String>, Boolean> tableFilter = new Func1<Set<String>, Boolean>() {
      @Override public Boolean call(Set<String> triggers) {
        return triggers.contains(table);
      }

      @Override public String toString() {
        return table;
      }
    };
    return createQuery(tableFilter, sql, args);
  }

  /**
   * See {@link #createQuery(String, String, String...)} for usage. This overload allows for
   * monitoring multiple tables for changes.
   *
   * @see SQLiteDatabase#rawQuery(String, String[])
   */
  @CheckResult
  public Observable<Query> createQuery(@NonNull final Iterable<String> tables, @NonNull String sql,
      @NonNull String... args) {
    Func1<Set<String>, Boolean> tableFilter = new Func1<Set<String>, Boolean>() {
      @Override public Boolean call(Set<String> triggers) {
        for (String table : tables) {
          if (triggers.contains(table)) {
            return true;
          }
        }
        return false;
      }

      @Override public String toString() {
        return tables.toString();
      }
    };
    return createQuery(tableFilter, sql, args);
  }

  @CheckResult
  private Observable<Query> createQuery(final Func1<Set<String>, Boolean> tableFilter,
      final String sql, final String... args) {
    if (transactions.get() != null) {
      throw new IllegalStateException("Cannot create observable query in transaction. "
          + "Use query() for a query inside a transaction.");
    }

    final Query query = new Query() {
      @Override public Cursor run() {
        if (transactions.get() != null) {
          throw new IllegalStateException("Cannot execute observable query in a transaction.");
        }
        return getReadableDatabase().rawQuery(sql, args);
      }

      @Override public String toString() {
        return sql;
      }
    };

    return triggers //
        .filter(tableFilter) // Only trigger on tables we care about.
        .startWith(INITIAL_TRIGGER) // Immediately execute the query for initial value.
        .map(new Func1<Set<String>, Query>() {
          @Override public Query call(Set<String> trigger) {
            if (transactions.get() != null) {
              throw new IllegalStateException(
                  "Cannot subscribe to observable query in a transaction.");
            }
            if (logging) {
              log("QUERY\n  trigger: %s\n  tables: %s\n  sql: %s\n  args: %s", trigger, tableFilter,
                  sql, Arrays.toString(args));
            }
            return query;
          }
        }) //
        .lift(BackpressureBufferLastOperator.<Query>instance());
  }

  /**
   * Runs the provided SQL and returns a {@link Cursor} over the result set.
   *
   * @see SQLiteDatabase#rawQuery(String, String[])
   */
  @CheckResult @WorkerThread
  public Cursor query(@NonNull String sql, @NonNull String... args) {
    if (logging) log("QUERY\n  sql: %s\n  args: %s", sql, Arrays.toString(args));
    return getReadableDatabase().rawQuery(sql, args);
  }

  /**
   * Insert a row into the specified {@code table} and notify any subscribed queries.
   *
   * @see SQLiteDatabase#insert(String, String, ContentValues)
   */
  @WorkerThread
  public long insert(@NonNull String table, @NonNull ContentValues values) {
    return insert(table, values, CONFLICT_NONE);
  }

  /**
   * Insert a row into the specified {@code table} and notify any subscribed queries.
   *
   * @see SQLiteDatabase#insertWithOnConflict(String, String, ContentValues, int)
   */
  @WorkerThread
  public long insert(@NonNull String table, @NonNull ContentValues values,
      @ConflictAlgorithm int conflictAlgorithm) {
    SQLiteDatabase db = getWriteableDatabase();

    if (logging) {
      log("INSERT\n  table: %s\n  values: %s\n  conflictAlgorithm: %s", table, values,
          conflictString(conflictAlgorithm));
    }
    long rowId = db.insertWithOnConflict(table, null, values, conflictAlgorithm);

    if (logging) log("INSERT id: %s", rowId);

    if (rowId != -1) {
      // Only send a table trigger if the insert was successful.
      sendTableTrigger(Collections.singleton(table));
    }
    return rowId;
  }

  /**
   * Delete rows from the specified {@code table} and notify any subscribed queries. This method
   * will not trigger a notification if no rows were deleted.
   *
   * @see SQLiteDatabase#delete(String, String, String[])
   */
  @WorkerThread
  public int delete(@NonNull String table, @Nullable String whereClause,
      @Nullable String... whereArgs) {
    SQLiteDatabase db = getWriteableDatabase();

    if (logging) {
      log("DELETE\n  table: %s\n  whereClause: %s\n  whereArgs: %s", table, whereClause,
          Arrays.toString(whereArgs));
    }
    int rows = db.delete(table, whereClause, whereArgs);

    if (logging) log("DELETE affected %s %s", rows, rows != 1 ? "rows" : "row");

    if (rows > 0) {
      // Only send a table trigger if rows were affected.
      sendTableTrigger(Collections.singleton(table));
    }
    return rows;
  }

  /**
   * Update rows in the specified {@code table} and notify any subscribed queries. This method
   * will not trigger a notification if no rows were updated.
   *
   * @see SQLiteDatabase#update(String, ContentValues, String, String[])
   */
  @WorkerThread
  public int update(@NonNull String table, @NonNull ContentValues values,
      @Nullable String whereClause, @Nullable String... whereArgs) {
    return update(table, values, CONFLICT_NONE, whereClause, whereArgs);
  }

  /**
   * Update rows in the specified {@code table} and notify any subscribed queries. This method
   * will not trigger a notification if no rows were updated.
   *
   * @see SQLiteDatabase#updateWithOnConflict(String, ContentValues, String, String[], int)
   */
  @WorkerThread
  public int update(@NonNull String table, @NonNull ContentValues values,
      @ConflictAlgorithm int conflictAlgorithm, @Nullable String whereClause,
      @Nullable String... whereArgs) {
    SQLiteDatabase db = getWriteableDatabase();

    if (logging) {
      log("UPDATE\n  table: %s\n  values: %s\n  whereClause: %s\n  whereArgs: %s\n  conflictAlgorithm: %s",
          table, values, whereClause, Arrays.toString(whereArgs),
          conflictString(conflictAlgorithm));
    }
    int rows = db.updateWithOnConflict(table, values, whereClause, whereArgs, conflictAlgorithm);

    if (logging) log("UPDATE affected %s %s", rows, rows != 1 ? "rows" : "row");

    if (rows > 0) {
      // Only send a table trigger if rows were affected.
      sendTableTrigger(Collections.singleton(table));
    }
    return rows;
  }

  /** An in-progress database transaction. */
  public interface Transaction extends Closeable {
    /**
     * End a transaction. See {@link #newTransaction()} for notes about how to use this and when
     * transactions are committed and rolled back.
     *
     * @see SQLiteDatabase#endTransaction()
     */
    void end();

    /**
     * Marks the current transaction as successful. Do not do any more database work between
     * calling this and calling {@link #end()}. Do as little non-database work as possible in that
     * situation too. If any errors are encountered between this and {@link #end()} the transaction
     * will still be committed.
     *
     * @see SQLiteDatabase#setTransactionSuccessful()
     */
    void markSuccessful();

    /**
     * Temporarily end the transaction to let other threads run. The transaction is assumed to be
     * successful so far. Do not call {@link #markSuccessful()} before calling this. When this
     * returns a new transaction will have been created but not marked as successful. This assumes
     * that there are no nested transactions (newTransaction has only been called once) and will
     * throw an exception if that is not the case.
     *
     * @return true if the transaction was yielded
     *
     * @see SQLiteDatabase#yieldIfContendedSafely()
     */
    @WorkerThread
    boolean yieldIfContendedSafely();

    /**
     * Temporarily end the transaction to let other threads run. The transaction is assumed to be
     * successful so far. Do not call {@link #markSuccessful()} before calling this. When this
     * returns a new transaction will have been created but not marked as successful. This assumes
     * that there are no nested transactions (newTransaction has only been called once) and will
     * throw an exception if that is not the case.
     *
     * @param sleepAmount if > 0, sleep this long before starting a new transaction if
     *   the lock was actually yielded. This will allow other background threads to make some
     *   more progress than they would if we started the transaction immediately.
     * @return true if the transaction was yielded
     *
     * @see SQLiteDatabase#yieldIfContendedSafely(long)
     */
    @WorkerThread
    boolean yieldIfContendedSafely(long sleepAmount, TimeUnit sleepUnit);
  }

  @IntDef({
      CONFLICT_ABORT,
      CONFLICT_FAIL,
      CONFLICT_IGNORE,
      CONFLICT_NONE,
      CONFLICT_REPLACE,
      CONFLICT_ROLLBACK
  })
  @Retention(SOURCE)
  public @interface ConflictAlgorithm {
  }

  private void log(String message, Object... args) {
    if (args.length > 0) message = String.format(message, args);
    logger.log(message);
  }

  private static String conflictString(@ConflictAlgorithm int conflictAlgorithm) {
    switch (conflictAlgorithm) {
      case CONFLICT_ABORT:
        return "abort";
      case CONFLICT_FAIL:
        return "fail";
      case CONFLICT_IGNORE:
        return "ignore";
      case CONFLICT_NONE:
        return "none";
      case CONFLICT_REPLACE:
        return "replace";
      case CONFLICT_ROLLBACK:
        return "rollback";
      default:
        return "unknown (" + conflictAlgorithm + ')';
    }
  }

  private final class SqliteTransaction implements SQLiteTransactionListener {
    final SqliteTransaction parent;
    final Set<String> triggers = new LinkedHashSet<>();

    SqliteTransaction(SqliteTransaction parent) {
      this.parent = parent;
    }

    @Override public void onBegin() {
    }

    @Override public void onCommit() {
      sendTableTrigger(triggers);
    }

    @Override public void onRollback() {
    }

    @Override public String toString() {
      String name = String.format("%08x", System.identityHashCode(this));
      return parent == null ? name : name + " [" + parent.toString() + ']';
    }
  }
}
