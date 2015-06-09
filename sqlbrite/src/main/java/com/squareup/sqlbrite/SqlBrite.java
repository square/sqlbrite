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
import android.support.annotation.NonNull;
import android.util.Log;

/**
 * A lightweight wrapper around {@link SQLiteOpenHelper} which allows for continuously observing
 * the result of a query.
 */
public final class SqlBrite {
  public static SqlBrite create() {
    return create(new Logger() {
      @Override public void log(String message) {
        Log.d("SqlBrite", message);
      }
    });
  }

  public static SqlBrite create(@NonNull Logger logger) {
    return new SqlBrite(logger);
  }

  private final Logger logger;

  private SqlBrite(@NonNull Logger logger) {
    this.logger = logger;
  }

  /**
   * Wrap a {@link SQLiteOpenHelper} for observable queries.
   * <p>
   * While not strictly required, instances of this class assume that they will be the only ones
   * interacting with the underlying {@link SQLiteOpenHelper} and it is required for automatic
   * notifications of table changes to work. See {@linkplain BriteDatabase#createQuery the
   * <code>query</code> method} for more information on that behavior.
   */
  public BriteDatabase wrapDatabaseHelper(@NonNull SQLiteOpenHelper helper) {
    return new BriteDatabase(helper, logger);
  }

  /** Wrap a {@link ContentResolver} for observable queries. */
  public BriteContentResolver wrapContentProvider(@NonNull ContentResolver contentResolver) {
    return new BriteContentResolver(contentResolver, logger);
  }

  /** An executable query. */
  public interface Query {
    /** Execute the query on the underlying database and return the resulting cursor. */
    Cursor run();
  }

  /** A simple indirection for logging debug messages. */
  public interface Logger {
    void log(String message);
  }
}
