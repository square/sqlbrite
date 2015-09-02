package com.squareup.sqlbrite;

import android.database.Cursor;
import rx.Observable;
import rx.Subscriber;
import rx.exceptions.Exceptions;
import rx.exceptions.OnErrorThrowable;
import rx.functions.Func1;

final class QueryToOneOperator<T> implements Observable.Operator<T, SqlBrite.Query> {
  private final Func1<Cursor, T> mapper;
  private final boolean emitNull;

  QueryToOneOperator(Func1<Cursor, T> mapper, boolean emitNull) {
    this.mapper = mapper;
    this.emitNull = emitNull;
  }

  @Override public Subscriber<? super SqlBrite.Query> call(final Subscriber<? super T> subscriber) {
    return new Subscriber<SqlBrite.Query>(subscriber) {
      @Override public void onNext(SqlBrite.Query query) {
        try {
          T item = null;
          Cursor cursor = query.run();
          try {
            if (cursor.moveToNext()) {
              item = mapper.call(cursor);
              if (item == null) {
                throw new NullPointerException("Mapper returned null for row 1");
              }
              if (cursor.moveToNext()) {
                throw new IllegalStateException("Cursor returned more than 1 row");
              }
            }
          } finally {
            cursor.close();
          }
          if (!subscriber.isUnsubscribed() && (item != null || emitNull)) {
            subscriber.onNext(item);
          }
        } catch (Throwable e) {
          Exceptions.throwIfFatal(e);
          onError(OnErrorThrowable.addValueAsLastCause(e, query.toString()));
        }
      }

      @Override public void onCompleted() {
        subscriber.onCompleted();
      }

      @Override public void onError(Throwable e) {
        subscriber.onError(e);
      }
    };
  }
}
