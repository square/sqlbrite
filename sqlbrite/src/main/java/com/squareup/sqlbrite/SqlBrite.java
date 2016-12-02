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

import android.content.ContentResolver;
import android.database.Cursor;
import android.database.sqlite.SQLiteOpenHelper;
import android.support.annotation.CheckResult;
import android.support.annotation.NonNull;
import android.support.annotation.Nullable;
import android.support.annotation.WorkerThread;
import android.util.Log;
import java.util.List;
import rx.Observable;
import rx.Observable.Operator;
import rx.Observable.Transformer;
import rx.Scheduler;
import rx.Subscriber;
import rx.functions.Func1;

/**
 * A lightweight wrapper around {@link SQLiteOpenHelper} which allows for continuously observing
 * the result of a query.
 */
public final class SqlBrite {
  static final Logger DEFAULT_LOGGER = new Logger() {
    @Override public void log(String message) {
      Log.d("SqlBrite", message);
    }
  };
  static final Transformer<Query, Query> DEFAULT_TRANSFORMER = new Transformer<Query, Query>() {
    @Override public Observable<Query> call(Observable<Query> queryObservable) {
      return queryObservable;
    }
  };

  public static final class Builder {
    private Logger logger = DEFAULT_LOGGER;
    private Transformer<Query, Query> queryTransformer = DEFAULT_TRANSFORMER;

    @CheckResult
    public Builder logger(@NonNull Logger logger) {
      if (logger == null) throw new NullPointerException("logger == null");
      this.logger = logger;
      return this;
    }

    @CheckResult
    public Builder queryTransformer(@NonNull Transformer<Query, Query> queryTransformer) {
      if (queryTransformer == null) throw new NullPointerException("queryTransformer == null");
      this.queryTransformer = queryTransformer;
      return this;
    }

    @CheckResult
    public SqlBrite build() {
      return new SqlBrite(logger, queryTransformer);
    }
  }

  /** @deprecated Use {@link Builder} to create instances. */
  @Deprecated @CheckResult @NonNull
  public static SqlBrite create() {
    return new SqlBrite(DEFAULT_LOGGER, DEFAULT_TRANSFORMER);
  }

  /** @deprecated Use {@link Builder} to create instances. */
  @Deprecated @CheckResult @NonNull
  public static SqlBrite create(@NonNull Logger logger) {
    if (logger == null) throw new NullPointerException("logger == null");
    return new SqlBrite(logger, DEFAULT_TRANSFORMER);
  }

  private final Logger logger;
  private final Transformer<Query, Query> queryTransformer;

  private SqlBrite(@NonNull Logger logger, @NonNull Transformer<Query, Query> queryTransformer) {
    this.logger = logger;
    this.queryTransformer = queryTransformer;
  }

  /**
   * Wrap a {@link SQLiteOpenHelper} for observable queries.
   * <p>
   * While not strictly required, instances of this class assume that they will be the only ones
   * interacting with the underlying {@link SQLiteOpenHelper} and it is required for automatic
   * notifications of table changes to work. See {@linkplain BriteDatabase#createQuery the
   * <code>query</code> method} for more information on that behavior.
   *
   * @param scheduler The {@link Scheduler} on which items from {@link BriteDatabase#createQuery}
   * will be emitted.
   */
  @CheckResult @NonNull public BriteDatabase wrapDatabaseHelper(@NonNull SQLiteOpenHelper helper,
      @NonNull Scheduler scheduler) {
    return new BriteDatabase(helper, logger, scheduler, queryTransformer);
  }

  /**
   * Wrap a {@link ContentResolver} for observable queries.
   *
   * @param scheduler The {@link Scheduler} on which items from
   * {@link BriteContentResolver#createQuery} will be emitted.
   */
  @CheckResult @NonNull public BriteContentResolver wrapContentProvider(
      @NonNull ContentResolver contentResolver, @NonNull Scheduler scheduler) {
    return new BriteContentResolver(contentResolver, logger, scheduler, queryTransformer);
  }

  /** An executable query. */
  public static abstract class Query {
    /**
     * Creates an {@linkplain Operator observable operator} which transforms a query returning a
     * single row to {@code T} using {@code mapper}.
     * <p>
     * It is an error for a query to pass through this operator with more than 1 row in its result
     * set. Use {@code LIMIT 1} on the underlying SQL query to prevent this. Result sets with 0 rows
     * do not emit an item.
     * <p>
     * This operator ignores {@code null} cursors returned from {@link #run()}.
     *
     * @param mapper Maps the current {@link Cursor} row to {@code T}. May not return null.
     */
    @CheckResult @NonNull
    public static <T> Operator<T, Query> mapToOne(@NonNull Func1<Cursor, T> mapper) {
      return new QueryToOneOperator<>(mapper, false, null);
    }

    /**
     * Creates an {@linkplain Operator observable operator} which transforms a query returning a
     * single row to {@code T} using {@code mapper}.
     * <p>
     * It is an error for a query to pass through this operator with more than 1 row in its result
     * set. Use {@code LIMIT 1} on the underlying SQL query to prevent this. Result sets with 0 rows
     * emit {@code defaultValue}.
     * <p>
     * This operator emits {@code defaultValue} if {@code null} is returned from {@link #run()}.
     *
     * @param mapper Maps the current {@link Cursor} row to {@code T}. May not return null.
     * @param defaultValue Value returned if result set is empty
     */
    @CheckResult @NonNull
    public static <T> Operator<T, Query> mapToOneOrDefault(@NonNull Func1<Cursor, T> mapper,
        T defaultValue) {
      return new QueryToOneOperator<>(mapper, true, defaultValue);
    }

    /**
     * Creates an {@linkplain Operator observable operator} which transforms a query to a
     * {@code List<T>} using {@code mapper}.
     * <p>
     * Be careful using this operator as it will always consume the entire cursor and create objects
     * for each row, every time this observable emits a new query. On tables whose queries update
     * frequently or very large result sets this can result in the creation of many objects.
     * <p>
     * This operator ignores {@code null} cursors returned from {@link #run()}.
     *
     * @param mapper Maps the current {@link Cursor} row to {@code T}. May not return null.
     */
    @CheckResult @NonNull
    public static <T> Operator<List<T>, Query> mapToList(@NonNull Func1<Cursor, T> mapper) {
      return new QueryToListOperator<>(mapper);
    }

    /**
     * Execute the query on the underlying database and return the resulting cursor.
     *
     * @return A {@link Cursor} with query results, or {@code null} when the query could not be
     * executed due to a problem with the underlying store. Unfortunately it is not well documented
     * when {@code null} is returned. It usually involves a problem in communicating with the
     * underlying store and should either be treated as failure or ignored for retry at a later
     * time.
     */
    @CheckResult @WorkerThread
    @Nullable
    public abstract Cursor run();

    /**
     * Execute the query on the underlying database and return an Observable of each row mapped to
     * {@code T} by {@code mapper}.
     * <p>
     * Standard usage of this operation is in {@code flatMap}:
     * <pre>{@code
     * flatMap(q -> q.asRows(Item.MAPPER).toList())
     * }</pre>
     * However, the above is a more-verbose but identical operation as
     * {@link QueryObservable#mapToList}. This {@code asRows} method should be used when you need
     * to limit or filter the items separate from the actual query.
     * <pre>{@code
     * flatMap(q -> q.asRows(Item.MAPPER).take(5).toList())
     * // or...
     * flatMap(q -> q.asRows(Item.MAPPER).filter(i -> i.isActive).toList())
     * }</pre>
     * <p>
     * Note: Limiting results or filtering will almost always be faster in the database as part of
     * a query and should be preferred, where possible.
     * <p>
     * The resulting observable will be empty if {@code null} is returned from {@link #run()}.
     */
    @CheckResult @NonNull
    public final <T> Observable<T> asRows(final Func1<Cursor, T> mapper) {
      return Observable.create(new Observable.OnSubscribe<T>() {
        @Override public void call(Subscriber<? super T> subscriber) {
          Cursor cursor = run();
          if (cursor != null) {
            try {
              while (cursor.moveToNext() && !subscriber.isUnsubscribed()) {
                subscriber.onNext(mapper.call(cursor));
              }
            } finally {
              cursor.close();
            }
          }
          if (!subscriber.isUnsubscribed()) {
            subscriber.onCompleted();
          }
        }
      });
    }
  }

  /** A simple indirection for logging debug messages. */
  public interface Logger {
    void log(String message);
  }
}
