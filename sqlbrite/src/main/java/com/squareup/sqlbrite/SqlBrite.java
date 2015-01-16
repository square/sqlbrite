package com.squareup.sqlbrite;

import android.content.ContentValues;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.support.annotation.IntDef;
import android.support.annotation.NonNull;
import android.support.annotation.Nullable;
import android.util.Log;
import java.io.Closeable;
import java.io.IOException;
import java.lang.annotation.Retention;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import rx.Observable;
import rx.Subscriber;
import rx.functions.Func1;
import rx.subjects.BehaviorSubject;
import rx.subjects.PublishSubject;

import static android.database.sqlite.SQLiteDatabase.CONFLICT_ABORT;
import static android.database.sqlite.SQLiteDatabase.CONFLICT_FAIL;
import static android.database.sqlite.SQLiteDatabase.CONFLICT_IGNORE;
import static android.database.sqlite.SQLiteDatabase.CONFLICT_NONE;
import static android.database.sqlite.SQLiteDatabase.CONFLICT_REPLACE;
import static android.database.sqlite.SQLiteDatabase.CONFLICT_ROLLBACK;
import static com.squareup.sqlbrite.BuildConfig.DEBUG;
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
  /** Emits a single item but never completes. */
  private static final Observable<String> INITIAL_TRIGGER = BehaviorSubject.create("<initial>");

  private final SQLiteOpenHelper helper;
  private final Map<String, PublishSubject<String>> tableTriggers = new LinkedHashMap<>();

  // Read and write guarded by 'this'. Lazily initialized. Use methods to access.
  private SQLiteDatabase readableDatabase;
  private SQLiteDatabase writeableDatabase;

  public SqlBrite(@NonNull SQLiteOpenHelper helper) {
    this.helper = helper;
  }

  private SQLiteDatabase getReadableDatabase() {
    SQLiteDatabase db = readableDatabase;
    if (db == null) {
      synchronized (this) {
        db = readableDatabase;
        if (db == null) {
          if (DEBUG) log("Creating readable database");
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
          if (DEBUG) log("Creating writeable database");
          db = writeableDatabase = helper.getWritableDatabase();
        }
      }
    }
    return db;
  }

  private Set<Observable<String>> getTableTriggers(Iterable<String> tables) {
    Set<Observable<String>> triggers = new LinkedHashSet<>();
    synchronized (this) {
      for (String table : tables) {
        PublishSubject<String> tableTrigger = tableTriggers.get(table);
        if (tableTrigger == null) {
          tableTrigger = PublishSubject.create();
          tableTriggers.put(table, tableTrigger);
        }
        triggers.add(tableTrigger);
      }
    }

    triggers.add(INITIAL_TRIGGER);
    return triggers;
  }

  private void sendTableTrigger(String table) {
    if (DEBUG) log("TRIGGER %s", table);

    PublishSubject<String> tableTrigger;
    synchronized (this) {
      tableTrigger = tableTriggers.get(table);
    }
    if (tableTrigger != null) {
      tableTrigger.onNext(table);
    }
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
  public Observable<Cursor> createQuery(@NonNull String table, @NonNull String sql,
      @NonNull String... args) {
    return createQuery(Collections.singleton(table), sql, args);
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
  public Observable<Cursor> createQuery(@NonNull final Iterable<String> tables,
      @NonNull final String sql, @NonNull final String... args) {
    return Observable.create(new Observable.OnSubscribe<Cursor>() {
      @Override public void call(Subscriber<? super Cursor> subscriber) {
        Observable.merge(getTableTriggers(tables))
            .map(new Func1<String, Cursor>() {
              @Override public Cursor call(String trigger) {
                if (DEBUG) {
                  log("QUERY\n  trigger: %s\n  tables: %s\n  sql: %s\n  args: %s", trigger, tables,
                      sql, Arrays.toString(args));
                }
                return getReadableDatabase().rawQuery(sql, args);
              }
            })
            .subscribe(subscriber);
      }
    });
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

    if (DEBUG) {
      log("INSERT\n  table: %s\n  values: %s\n  conflictAlgorithm: %s", table, values,
          conflictString(conflictAlgorithm));
    }
    long rowId = db.insertWithOnConflict(table, null, values, conflictAlgorithm);

    if (DEBUG) log("INSERT id: %s", rowId);

    if (rowId != -1) {
      // Only send a table trigger if the insert was successful.
      sendTableTrigger(table);
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

    if (DEBUG) {
      log("DELETE\n  table: %s\n  whereClause: %s\n  whereArgs: %s", table, whereClause,
          Arrays.toString(whereArgs));
    }
    int rows = db.delete(table, whereClause, whereArgs);

    if (DEBUG) log("DELETE affected %s %s", rows, rows != 1 ? "rows" : "row");

    if (rows > 0) {
      // Only send a table trigger if rows were affected.
      sendTableTrigger(table);
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

    if (DEBUG) {
      log("UPDATE\n  table: %s\n  values: %s\n  whereClause: %s\n  whereArgs: %s\n  conflictAlgorithm: %s",
          table, values, whereClause, Arrays.toString(whereArgs),
          conflictString(conflictAlgorithm));
    }
    int rows = db.updateWithOnConflict(table, values, whereClause, whereArgs, conflictAlgorithm);

    if (DEBUG) log("UPDATE affected %s %s", rows, rows != 1 ? "rows" : "row");

    if (rows > 0) {
      // Only send a table trigger if rows were affected.
      sendTableTrigger(table);
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

  private static void log(String message, Object... args) {
    if (args.length > 0) message = String.format(message, args);
    Log.d("SqlBrite", message);
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
}
