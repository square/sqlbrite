package com.squareup.sqlbrite;

import android.database.Cursor;
import android.util.Log;
import java.util.ArrayDeque;
import java.util.Deque;
import rx.Observer;

import static com.google.common.truth.Truth.assertThat;

final class RecordingObserver implements Observer<Cursor> {
  private static final Object COMPLETED = "<completed>";
  private static final String TAG = RecordingObserver.class.getSimpleName();

  private final Deque<Object> events = new ArrayDeque<>();

  @Override public void onCompleted() {
    Log.d(TAG, "onCompleted");
    events.add(COMPLETED);
  }

  @Override public void onError(Throwable e) {
    Log.d(TAG, "onError " + e.getClass().getSimpleName() + " " + e.getMessage());
    events.add(e);
  }

  @Override public void onNext(Cursor value) {
    Log.d(TAG, "onNext " + value);
    events.add(value);
  }

  public CursorAssert assertCursor() {
    Object event = events.pollFirst();
    assertThat(event).isInstanceOf(Cursor.class);
    return new CursorAssert((Cursor) event);
  }

  public void assertError(String expected) {
    Object event = events.removeFirst();
    assertThat(event).isInstanceOf(Throwable.class);
    assertThat((Throwable) event).hasMessage(expected);
  }

  public void assertNoMoreEvents() {
    assertThat(events).isEmpty();
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
      assertThat(cursor.moveToNext()).named("more than " + row + " rows").isFalse();
      cursor.close();
    }
  }
}
