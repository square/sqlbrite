package com.squareup.sqlbrite;

import android.database.Cursor;
import android.database.MatrixCursor;
import android.support.test.runner.AndroidJUnit4;

import com.squareup.sqlbrite.SqlBrite.Query;

import org.junit.Test;
import org.junit.runner.RunWith;

import rx.Observable.OnSubscribe;
import rx.Subscriber;
import rx.functions.Func1;
import rx.observers.TestSubscriber;

import static com.google.common.truth.Truth.assertThat;

@RunWith(AndroidJUnit4.class)
public final class QueryObservableTest {

  @Test public void mapToListThrowsFromQueryRun() {
    TestSubscriber<Object> testSubscriber = new TestSubscriber<>();

    new QueryObservable(new OnSubscribe<Query>() {
      @Override public void call(Subscriber<? super Query> subscriber) {
        subscriber.onNext(new Query() {
          @Override public Cursor run() {
            throw new IllegalStateException("test exception");
          }
        });
      }
    }).mapToList(new Func1<Cursor, Object>() {
      @Override public Object call(Cursor cursor) {
        throw new AssertionError("Must not be called");
      }
    }).subscribe(testSubscriber);

    testSubscriber.awaitTerminalEvent();
    testSubscriber.assertNoValues();
    assertThat(testSubscriber.getOnErrorEvents()).hasSize(1);

    IllegalStateException expected = (IllegalStateException) testSubscriber.getOnErrorEvents().get(0);
    assertThat(expected).hasMessage("test exception");
  }

  @Test public void mapToListThrowsFromMapFunction() {
    TestSubscriber<Object> testSubscriber = new TestSubscriber<>();

    new QueryObservable(new OnSubscribe<Query>() {
      @Override public void call(Subscriber<? super Query> subscriber) {
        subscriber.onNext(new Query() {
          @Override public Cursor run() {
            MatrixCursor cursor = new MatrixCursor(new String[]{"col1"});
            cursor.addRow(new Object[]{"value1"});
            return cursor;
          }
        });
      }
    }).mapToList(new Func1<Cursor, Object>() {
      @Override public Object call(Cursor cursor) {
        throw new IllegalStateException("test exception");
      }
    }).subscribe(testSubscriber);

    testSubscriber.awaitTerminalEvent();
    testSubscriber.assertNoValues();
    assertThat(testSubscriber.getOnErrorEvents()).hasSize(1);

    IllegalStateException expected = (IllegalStateException) testSubscriber.getOnErrorEvents().get(0);
    assertThat(expected).hasMessage("test exception");
  }
}
