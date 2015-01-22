package com.squareup.sqlbrite;

import android.database.Cursor;
import android.util.Log;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import rx.Observer;

import static com.google.common.truth.Truth.assertThat;
import static java.util.concurrent.TimeUnit.SECONDS;

final class RecordingObserver implements Observer<Cursor> {
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

  @Override public void onNext(Cursor value) {
    Log.d(TAG, "onNext " + value);
    events.add(value);
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
