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

import android.database.Cursor;
import android.util.Log;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import rx.Subscriber;

import static com.google.common.truth.Truth.assertThat;
import static com.squareup.sqlbrite.SqlBrite.Query;
import static java.util.concurrent.TimeUnit.SECONDS;

final class RecordingObserver extends Subscriber<Query> {
  private static final Object COMPLETED = "<completed>";
  private static final String TAG = RecordingObserver.class.getSimpleName();

  private final BlockingDeque<Object> events = new LinkedBlockingDeque<>();

  @Override public void onCompleted() {
    Log.d(TAG, "onCompleted");
    events.add(COMPLETED);
  }

  @Override public void onError(Throwable e) {
    Log.d(TAG, "onError " + e.getClass().getSimpleName() + " " + e.getMessage());
    events.add(e);
  }

  @Override public void onNext(Query value) {
    Log.d(TAG, "onNext " + value);
    events.add(value.run());
  }

  public void doRequest(long amount) {
    request(amount);
  }

  private Object takeEvent() {
    try {
      Object item = events.pollFirst(1, SECONDS);
      if (item == null) {
        throw new AssertionError("Timeout expired waiting for item.");
      }
      return item;
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public CursorAssert assertCursor() {
    Object event = takeEvent();
    assertThat(event).isInstanceOf(Cursor.class);
    return new CursorAssert((Cursor) event);
  }

  public void assertErrorContains(String expected) {
    Object event = takeEvent();
    assertThat(event).isInstanceOf(Throwable.class);
    assertThat(((Throwable) event).getMessage()).contains(expected);
  }

  public void assertNoMoreEvents() {
    try {
      assertThat(events.pollFirst(1, SECONDS)).isNull();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  static final class CursorAssert {
    private final Cursor cursor;
    private int row = 0;

    CursorAssert(Cursor cursor) {
      this.cursor = cursor;
    }

    public CursorAssert hasRow(Object... values) {
      assertThat(cursor.moveToNext()).named("row " + (row + 1) + " exists").isTrue();
      row += 1;
      assertThat(cursor.getColumnCount()).named("column count").isEqualTo(values.length);
      for (int i = 0; i < values.length; i++) {
        assertThat(cursor.getString(i))
            .named("row " + row + " column '" + cursor.getColumnName(i) + "'")
            .isEqualTo(values[i]);
      }
      return this;
    }

    public void isExhausted() {
      if (cursor.moveToNext()) {
        StringBuilder data = new StringBuilder();
        for (int i = 0; i < cursor.getColumnCount(); i++) {
          if (i > 0) data.append(", ");
          data.append(cursor.getString(i));
        }
        throw new AssertionError("Expected no more rows but was: " + data);
      }
      cursor.close();
    }
  }
}
