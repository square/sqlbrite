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
package com.squareup.sqlbrite2;

import android.database.Cursor;
import android.os.Build;
import android.support.annotation.RequiresApi;
import io.reactivex.ObservableOperator;
import io.reactivex.Observer;
import io.reactivex.exceptions.Exceptions;
import io.reactivex.functions.Function;
import io.reactivex.observers.DisposableObserver;
import io.reactivex.plugins.RxJavaPlugins;
import java.util.Optional;

@RequiresApi(Build.VERSION_CODES.N)
final class QueryToOptionalOperator<T> implements ObservableOperator<Optional<T>, SqlBrite.Query> {
  private final Function<Cursor, T> mapper;

  QueryToOptionalOperator(Function<Cursor, T> mapper) {
    this.mapper = mapper;
  }

  @Override public Observer<? super SqlBrite.Query> apply(Observer<? super Optional<T>> observer) {
    return new MappingObserver<>(observer, mapper);
  }

  static final class MappingObserver<T> extends DisposableObserver<SqlBrite.Query> {
    private final Observer<? super Optional<T>> downstream;
    private final Function<Cursor, T> mapper;

    MappingObserver(Observer<? super Optional<T>> downstream, Function<Cursor, T> mapper) {
      this.downstream = downstream;
      this.mapper = mapper;
    }

    @Override protected void onStart() {
      downstream.onSubscribe(this);
    }

    @Override public void onNext(SqlBrite.Query query) {
      try {
        T item = null;
        Cursor cursor = query.run();
        if (cursor != null) {
          try {
            if (cursor.moveToNext()) {
              item = mapper.apply(cursor);
              if (item == null) {
                downstream.onError(new NullPointerException("QueryToOne mapper returned null"));
                return;
              }
              if (cursor.moveToNext()) {
                throw new IllegalStateException("Cursor returned more than 1 row");
              }
            }
          } finally {
            cursor.close();
          }
        }
        if (!isDisposed()) {
          downstream.onNext(Optional.ofNullable(item));
        }
      } catch (Throwable e) {
        Exceptions.throwIfFatal(e);
        onError(e);
      }
    }

    @Override public void onComplete() {
      if (!isDisposed()) {
        downstream.onComplete();
      }
    }

    @Override public void onError(Throwable e) {
      if (isDisposed()) {
        RxJavaPlugins.onError(e);
      } else {
        downstream.onError(e);
      }
    }
  }
}
