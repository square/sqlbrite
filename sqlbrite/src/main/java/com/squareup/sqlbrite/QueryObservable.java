package com.squareup.sqlbrite;

import android.database.Cursor;
import android.support.annotation.CheckResult;
import android.support.annotation.NonNull;
import com.squareup.sqlbrite.SqlBrite.Query;
import java.util.ArrayList;
import java.util.List;
import rx.Observable;
import rx.Subscriber;
import rx.functions.Func1;

/** An {@link Observable} of {@link Query} which offers query-specific convenience operators. */
public final class QueryObservable extends Observable<Query> {
  QueryObservable(final Observable<Query> o) {
    super(new OnSubscribe<Query>() {
      @Override public void call(Subscriber<? super Query> subscriber) {
        o.unsafeSubscribe(subscriber);
      }
    });
  }

  /**
   * Given a function mapping the current row of a {@link Cursor} to {@code T}, transform each
   * emitted {@link Query} to a {@code List<T>}.
   * <p>
   * Be careful using this operator as it will always consume the entire cursor and create objects
   * for each row, every time this observable emits a new query. On tables whose queries update
   * frequently or very large result sets this can result in the creation of many objects.
   * <p>
   * This method is equivalent to:
   * <pre>{@code
   * flatMap(q -> q.asRows(Item.MAPPER).toList())
   * }</pre>
   * Consider using {@link Query#asRows} if you need to limit or filter in memory.
   */
  @CheckResult
  public final <T> Observable<List<T>> mapToList(@NonNull final Func1<Cursor, T> mapper) {
    return lift(new Operator<List<T>, Query>() {
      @Override
      public Subscriber<? super Query> call(final Subscriber<? super List<T>> subscriber) {
        return new Subscriber<Query>(subscriber) {
          @Override public void onNext(Query query) {
            Cursor cursor = query.run();
            List<T> items = new ArrayList<>(cursor.getCount());
            try {
              while (cursor.moveToNext() && !subscriber.isUnsubscribed()) {
                items.add(mapper.call(cursor));
              }
            } finally {
              cursor.close();
            }
            if (!subscriber.isUnsubscribed()) {
              subscriber.onNext(items);
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
    });
  }
}
