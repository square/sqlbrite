package com.squareup.sqlbrite;

import android.content.ContentValues;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteTransactionListener;
import android.support.annotation.IntDef;
import android.support.annotation.NonNull;
import android.support.annotation.Nullable;
import android.util.Log;
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
 * the result of a query.
 * <p>
 * While not strictly required, instances of this class assume that they will be the only ones
 * interacting with the underlying {@link SQLiteOpenHelper} and it is required for automatic
 * notifications of table changes to work. See {@linkplain #createQuery the <code>query</code>
 * method} for more information on that behavior.
 */
public final class SqlBrite implements Closeable {
  private static final Set<String> INITIAL_TRIGGER = Collections.singleton("<initial>");

  /** Create an instance around the specified {@code helper} using appropriate defaults. */
  public static SqlBrite create(@NonNull SQLiteOpenHelper helper) {
    return builder(helper).build();
  }

  /** Build a new instance around the specified {@code helper}. */
  public static Builder builder(@NonNull SQLiteOpenHelper helper) {
    return new Builder(helper);
  }

  /** A simple indirection for logging debug messages. */
  public interface Logger {
    void log(String message);
  }

  public static final class Builder {
    private final SQLiteOpenHelper helper;
    private boolean logging;
    private Logger logger;
    private long throttleAmount = 500;
    private TimeUnit throttleUnit = TimeUnit.MILLISECONDS;

    private Builder(SQLiteOpenHelper helper) {
      this.helper = helper;
    }

    /**
     * Control whether debug logs for transactions, queries, and notifications are recorded.
     * <p>
     * Default: {@code false}
     */
    public Builder loggingEnabled(boolean enabled) {
      logging = enabled;
      return this;
    }

    /**
     * Set the target for {@link #logging} messages.
     * <p>
     * Default: {@link Log}
     */
    public Builder logger(Logger logger) {
      this.logger = logger;
      return this;
    }

    /**
     * Throttle query notifications after the initial result. This value will prevent your
     * subscriber from being called rapidly when multiple database interactions modify data.
     * <p>
     * <b>Note:</b> This feature means that {@linkplain #logging debug logs} will only show
     * the most recent trigger for a query.
     * <p>
     * Default: 500 milliseconds.
     */
    public Builder throttle(long amount, TimeUnit unit) {
      if (amount < 0) throw new IllegalArgumentException("amount < 0");
      if (unit == null) throw new NullPointerException("unit == null");
      throttleAmount = amount;
      throttleUnit = unit;
      return this;
    }

    public SqlBrite build() {
      if (logging && logger == null) {
        logger = new Logger() {
          @Override public void log(String message) {
            Log.d("SqlBrite", message);
          }
        };
      }
      return new SqlBrite(this);
    }
  }

  private final SQLiteOpenHelper helper;
  private final boolean logging;
  private final Logger logger;
  private final long throttleAmount;
  private final TimeUnit throttleUnit;

  private final ThreadLocal<Transaction> transactions = new ThreadLocal<>();
  /** Publishes sets of tables which have changed. */
  private final PublishSubject<Set<String>> trigger = PublishSubject.create();

  // Read and write guarded by 'this'. Lazily initialized. Use methods to access.
  private SQLiteDatabase readableDatabase;
  private SQLiteDatabase writeableDatabase;

  private SqlBrite(Builder builder) {
    helper = builder.helper;
    logging = builder.logging;
    logger = builder.logger;
    throttleAmount = builder.throttleAmount;
    throttleUnit = builder.throttleUnit;
  }

  private SQLiteDatabase getReadableDatabase() {
    SQLiteDatabase db = readableDatabase;
    if (db == null) {
      synchronized (this) {
        db = readableDatabase;
        if (db == null) {
          if (logging) log("Creating readable database");
          db = readableDatabase = helper.getReadableDatabase();
        }
      }
    }
    return db;
  }

  private SQLiteDatabase getWriteableDatabase() {
    SQLiteDatabase db = writeableDatabase;
    if (db == null) {
      synchronized (this) {
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
    Transaction transaction = transactions.get();
    if (transaction != null) {
      transaction.triggers.addAll(tables);
    } else {
      if (logging) log("TRIGGER %s", tables);
      trigger.onNext(tables);
    }
  }

  /**
   * Begin a transaction for this thread.
   * <p>
   * Transactions may nest. If the transaction is not in progress, then a database connection is
   * obtained and a new transaction is started. Otherwise, a nested transaction is started.
   * <p>
   * Each call to {@code beginTransaction} must be matched exactly by a call to
   * {@link #endTransaction}. To mark a transaction as successful, call
   * {@link #setTransactionSuccessful} before calling {@link #endTransaction}. If the transaction
   * is not successful, or if any of its nested transactions were not successful, then the entire
   * transaction will be rolled back when the outermost transaction is ended.
   * <p>
   * Transactions queue up all query notifications until they have been applied.
   * <p>
   * Here is the standard idiom for transactions:
   *
   * <pre>{@code
   * db.beginTransaction();
   * try {
   *   ...
   *   db.setTransactionSuccessful();
   * } finally {
   *   db.endTransaction();
   * }
   * }</pre>
   *
   * @see SQLiteDatabase#beginTransaction()
   */
  public void beginTransaction() {
    Transaction transaction = new Transaction(transactions.get());
    transactions.set(transaction);
    if (logging) log("TXN BEGIN %s", transaction);
    getWriteableDatabase().beginTransactionWithListener(transaction);
  }

  /**
   * Marks the current transaction as successful. Do not do any more database work between
   * calling this and calling {@link #endTransaction()}. Do as little non-database work as possible
   * in that situation too. If any errors are encountered between this and
   * {@link #endTransaction()} the transaction will still be committed.
   *
   * @see SQLiteDatabase#setTransactionSuccessful()
   */
  public void setTransactionSuccessful() {
    if (logging) log("TXN SUCCESS %s", transactions.get());
    getWriteableDatabase().setTransactionSuccessful();
  }

  /**
   * End a transaction. See {@link #beginTransaction()} for notes about how to use this and when
   * transactions are committed and rolled back.
   *
   * @see SQLiteDatabase#endTransaction()
   */
  public void endTransaction() {
    Transaction transaction = transactions.get();
    if (transaction == null) {
      throw new IllegalStateException("Not in transaction.");
    }
    Transaction newTransaction = transaction.parent;
    transactions.set(newTransaction);
    if (logging) log("TXN END %s", transaction);
    getWriteableDatabase().endTransaction();
  }

  /**
   * Close the underlying {@link SQLiteOpenHelper} and remove cached readable and writeable
   * databases. This does not prevent existing observables from retaining existing references as
   * well as attempting to create new ones for new subscriptions.
   */
  @Override public synchronized void close() throws IOException {
    helper.close();
    readableDatabase = null;
    writeableDatabase = null;
  }

  /**
   * Create an observable which will run the {@code sql} query (using the optional {@code args}
   * replacement values) and notify the subscriber with a {@linkplain Cursor cursor} for reading
   * the result. Subscribers are responsible for always closing the cursor.
   * <p>
   * Subscribers will automatically be notified with a new cursor if the specified {@code table}
   * changes through the {@code insert}, {@code update}, and {@code delete} methods of this class.
   * Unsubscribe when you no longer want updates to a query.
   * <p>
   * <b>Warning:</b> this method does not perform the query! Only by subscribing to the returned
   * {@link Observable} will the operation occur.
   *
   * @see SQLiteDatabase#rawQuery(String, String[])
   */
  public Observable<Cursor> createQuery(@NonNull final String table, @NonNull String sql,
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
   * Create an observable which will run the {@code sql} query (using the optional {@code args}
   * replacement values) and notify the subscriber with a {@linkplain Cursor cursor} for reading
   * the result. Subscribers are responsible for always closing the cursor.
   * <p>
   * Subscribers will automatically be notified with a new cursor if the specified {@code tables}
   * change through the {@code insert}, {@code update}, and {@code delete} methods of this class.
   * Unsubscribe when you no longer want updates to a query.
   * <p>
   * <b>Warning:</b> this method does not perform the query! Only by subscribing to the returned
   * {@link Observable} will the operation occur.
   *
   * @see SQLiteDatabase#rawQuery(String, String[])
   */
  public Observable<Cursor> createQuery(@NonNull final Iterable<String> tables, @NonNull String sql,
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

  private Observable<Cursor> createQuery(final Func1<Set<String>, Boolean> tableFilter,
      final String sql, final String... args) {
    if (transactions.get() != null) {
      throw new IllegalStateException("Cannot create observable query in transaction. "
          + "Use query() for a query inside a transaction.");
    }

    return trigger //
        .filter(tableFilter) // Only trigger on tables we care about.
        .throttleLast(throttleAmount, throttleUnit) // Ensure triggers don't spam us.
        .startWith(INITIAL_TRIGGER) // Immediately execute the query for initial value.
        .map(new Func1<Set<String>, Cursor>() {
          @Override public Cursor call(Set<String> trigger) {
            if (transactions.get() != null) {
              throw new IllegalStateException(
                  "Cannot subscribe to observable query in a transaction.");
            }

            if (logging) {
              log("QUERY\n  trigger: %s\n  tables: %s\n  sql: %s\n  args: %s", trigger, tableFilter,
                  sql, Arrays.toString(args));
            }
            return getReadableDatabase().rawQuery(sql, args);
          }
        });
  }

  /**
   * Runs the provided SQL and returns a {@link Cursor} over the result set.
   *
   * @see SQLiteDatabase#rawQuery(String, String[])
   */
  public Cursor query(@NonNull String sql, @NonNull String... args) {
    if (logging) log("QUERY\n  sql: %s\n  args: %s", sql, Arrays.toString(args));
    return getReadableDatabase().rawQuery(sql, args);
  }

  /**
   * Insert a row into the specified {@code table} and notify any subscribed queries.
   *
   * @see SQLiteDatabase#insert(String, String, ContentValues)
   */
  public long insert(@NonNull String table, @NonNull ContentValues values) {
    return insert(table, values, CONFLICT_NONE);
  }

  /**
   * Insert a row into the specified {@code table} and notify any subscribed queries.
   *
   * @see SQLiteDatabase#insertWithOnConflict(String, String, ContentValues, int)
   */
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

  private final class Transaction implements SQLiteTransactionListener {
    final Transaction parent;
    final Set<String> triggers = new LinkedHashSet<>();

    Transaction(Transaction parent) {
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
      String name = String.format("%08x", System.identityHashCode(this)).replace(' ', '0');
      return parent == null ? name : name + " [" + parent.toString() + ']';
    }
  }
}
