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
package com.squareup.sqlbrite3;

import android.arch.persistence.db.SupportSQLiteOpenHelper;
import android.content.ContentResolver;
import android.database.Cursor;
import android.os.Build;
import android.support.annotation.CheckResult;
import android.support.annotation.NonNull;
import android.support.annotation.Nullable;
import android.support.annotation.RequiresApi;
import android.support.annotation.WorkerThread;
import android.util.Log;
import io.reactivex.Observable;
import io.reactivex.ObservableEmitter;
import io.reactivex.ObservableOnSubscribe;
import io.reactivex.ObservableOperator;
import io.reactivex.ObservableTransformer;
import io.reactivex.Scheduler;
import io.reactivex.functions.Function;
import java.util.List;
import java.util.Optional;

/**
 * A lightweight wrapper around {@link SupportSQLiteOpenHelper} which allows for continuously
 * observing the result of a query.
 */
public final class SqlBrite {
  static final Logger DEFAULT_LOGGER = new Logger() {
    @Override public void log(String message) {
      Log.d("SqlBrite", message);
    }
  };
  static final ObservableTransformer<Query, Query> DEFAULT_TRANSFORMER =
      new ObservableTransformer<Query, Query>() {
        @Override public Observable<Query> apply(Observable<Query> queryObservable) {
          return queryObservable;
        }
      };

  public static final class Builder {
    private Logger logger = DEFAULT_LOGGER;
    private ObservableTransformer<Query, Query> queryTransformer = DEFAULT_TRANSFORMER;

    @CheckResult
    public Builder logger(@NonNull Logger logger) {
      if (logger == null) throw new NullPointerException("logger == null");
      this.logger = logger;
      return this;
    }

    @CheckResult
    public Builder queryTransformer(@NonNull ObservableTransformer<Query, Query> queryTransformer) {
      if (queryTransformer == null) throw new NullPointerException("queryTransformer == null");
      this.queryTransformer = queryTransformer;
      return this;
    }

    @CheckResult
    public SqlBrite build() {
      return new SqlBrite(logger, queryTransformer);
    }
  }

  final Logger logger;
  final ObservableTransformer<Query, Query> queryTransformer;

  SqlBrite(@NonNull Logger logger, @NonNull ObservableTransformer<Query, Query> queryTransformer) {
    this.logger = logger;
    this.queryTransformer = queryTransformer;
  }

  /**
   * Wrap a {@link SupportSQLiteOpenHelper} for observable queries.
   * <p>
   * While not strictly required, instances of this class assume that they will be the only ones
   * interacting with the underlying {@link SupportSQLiteOpenHelper} and it is required for
   * automatic notifications of table changes to work. See {@linkplain BriteDatabase#createQuery the
   * <code>query</code> method} for more information on that behavior.
   *
   * @param scheduler The {@link Scheduler} on which items from {@link BriteDatabase#createQuery}
   * will be emitted.
   */
  @CheckResult @NonNull public BriteDatabase wrapDatabaseHelper(
      @NonNull SupportSQLiteOpenHelper helper,
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
     * Creates an {@linkplain ObservableOperator operator} which transforms a query returning a
     * single row to a {@code T} using {@code mapper}. Use with {@link Observable#lift}.
     * <p>
     * It is an error for a query to pass through this operator with more than 1 row in its result
     * set. Use {@code LIMIT 1} on the underlying SQL query to prevent this. Result sets with 0 rows
     * do not emit an item.
     * <p>
     * This operator ignores {@code null} cursors returned from {@link #run()}.
     *
     * @param mapper Maps the current {@link Cursor} row to {@code T}. May not return null.
     */
    @CheckResult @NonNull //
    public static <T> ObservableOperator<T, Query> mapToOne(@NonNull Function<Cursor, T> mapper) {
      return new QueryToOneOperator<>(mapper, null);
    }

    /**
     * Creates an {@linkplain ObservableOperator operator} which transforms a query returning a
     * single row to a {@code T} using {@code mapper}. Use with {@link Observable#lift}.
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
    @SuppressWarnings("ConstantConditions") // Public API contract.
    @CheckResult @NonNull
    public static <T> ObservableOperator<T, Query> mapToOneOrDefault(
        @NonNull Function<Cursor, T> mapper, @NonNull T defaultValue) {
      if (defaultValue == null) throw new NullPointerException("defaultValue == null");
      return new QueryToOneOperator<>(mapper, defaultValue);
    }

    /**
     * Creates an {@linkplain ObservableOperator operator} which transforms a query returning a
     * single row to a {@code Optional<T>} using {@code mapper}. Use with {@link Observable#lift}.
     * <p>
     * It is an error for a query to pass through this operator with more than 1 row in its result
     * set. Use {@code LIMIT 1} on the underlying SQL query to prevent this. Result sets with 0 rows
     * emit {@link Optional#empty() Optional.empty()}.
     * <p>
     * This operator ignores {@code null} cursors returned from {@link #run()}.
     *
     * @param mapper Maps the current {@link Cursor} row to {@code T}. May not return null.
     */
    @RequiresApi(Build.VERSION_CODES.N) //
    @CheckResult @NonNull //
    public static <T> ObservableOperator<Optional<T>, Query> mapToOptional(
        @NonNull Function<Cursor, T> mapper) {
      return new QueryToOptionalOperator<>(mapper);
    }

    /**
     * Creates an {@linkplain ObservableOperator operator} which transforms a query to a
     * {@code List<T>} using {@code mapper}. Use with {@link Observable#lift}.
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
    public static <T> ObservableOperator<List<T>, Query> mapToList(
        @NonNull Function<Cursor, T> mapper) {
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
    public final <T> Observable<T> asRows(final Function<Cursor, T> mapper) {
      return Observable.create(new ObservableOnSubscribe<T>() {
        @Override public void subscribe(ObservableEmitter<T> e) throws Exception {
          Cursor cursor = run();
          if (cursor != null) {
            try {
              while (cursor.moveToNext() && !e.isDisposed()) {
                e.onNext(mapper.apply(cursor));
              }
            } finally {
              cursor.close();
            }
          }
          if (!e.isDisposed()) {
            e.onComplete();
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
