package com.squareup.sqlbrite;

import android.database.Cursor;
import java.util.ArrayList;
import java.util.List;
import rx.Observable;
import rx.Subscriber;
import rx.exceptions.Exceptions;
import rx.exceptions.OnErrorThrowable;
import rx.functions.Func1;

final class QueryToListOperator<T> implements Observable.Operator<List<T>, SqlBrite.Query> {
  final Func1<Cursor, T> mapper;

  QueryToListOperator(Func1<Cursor, T> mapper) {
    this.mapper = mapper;
  }

  @Override
  public Subscriber<? super SqlBrite.Query> call(final Subscriber<? super List<T>> subscriber) {
    return new Subscriber<SqlBrite.Query>(subscriber) {
      @Override public void onNext(SqlBrite.Query query) {
        try {
          Cursor cursor = query.run();
          if (cursor == null) {
            return;
          }
          List<T> items = new ArrayList<>(cursor.getCount());
          try {
            for (int i = 1; cursor.moveToNext() && !subscriber.isUnsubscribed(); i++) {
              T item = mapper.call(cursor);
              if (item == null) {
                throw new NullPointerException("Mapper returned null for row " + i);
              }
              items.add(item);
            }
          } finally {
            cursor.close();
          }
          if (!subscriber.isUnsubscribed()) {
            subscriber.onNext(items);
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
