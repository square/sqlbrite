/*
 * Copyright (C) 2017 Square, Inc.
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

import android.database.Cursor;
import android.database.MatrixCursor;
import com.squareup.sqlbrite3.QueryObservable;
import com.squareup.sqlbrite3.SqlBrite.Query;
import io.reactivex.Observable;
import io.reactivex.functions.Function;
import org.junit.Test;

public final class QueryObservableTest {
  @Test public void mapToListThrowsFromQueryRun() {
    final IllegalStateException error = new IllegalStateException("test exception");
    Query query = new Query() {
      @Override public Cursor run() {
        throw error;
      }
    };
    new QueryObservable(Observable.just(query)) //
        .mapToList(new Function<Cursor, Object>() {
          @Override public Object apply(Cursor cursor) {
            throw new AssertionError("Must not be called");
          }
        }) //
        .test() //
        .assertNoValues() //
        .assertError(error);
  }

  @Test public void mapToListThrowsFromMapFunction() {
    Query query = new Query() {
      @Override public Cursor run() {
        MatrixCursor cursor = new MatrixCursor(new String[] { "col1" });
        cursor.addRow(new Object[] { "value1" });
        return cursor;
      }
    };

    final IllegalStateException error = new IllegalStateException("test exception");
    new QueryObservable(Observable.just(query)) //
        .mapToList(new Function<Cursor, Object>() {
          @Override public Object apply(Cursor cursor) {
            throw error;
          }
        }) //
        .test() //
        .assertNoValues() //
        .assertError(error);
  }
}
