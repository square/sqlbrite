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
import io.reactivex.ObservableOperator;
import io.reactivex.Observer;
import io.reactivex.exceptions.Exceptions;
import io.reactivex.functions.Function;
import io.reactivex.observers.DisposableObserver;
import io.reactivex.plugins.RxJavaPlugins;
import java.util.ArrayList;
import java.util.List;

final class QueryToListOperator<T> implements ObservableOperator<List<T>, SqlBrite.Query> {
  private final Function<Cursor, T> mapper;

  QueryToListOperator(Function<Cursor, T> mapper) {
    this.mapper = mapper;
  }

  @Override public Observer<? super SqlBrite.Query> apply(Observer<? super List<T>> observer) {
    return new MappingObserver<>(observer, mapper);
  }

  static final class MappingObserver<T> extends DisposableObserver<SqlBrite.Query> {
    private final Observer<? super List<T>> downstream;
    private final Function<Cursor, T> mapper;

    MappingObserver(Observer<? super List<T>> downstream, Function<Cursor, T> mapper) {
      this.downstream = downstream;
      this.mapper = mapper;
    }

    @Override protected void onStart() {
      downstream.onSubscribe(this);
    }

    @Override public void onNext(SqlBrite.Query query) {
      try {
        Cursor cursor = query.run();
        if (cursor == null || isDisposed()) {
          return;
        }
        List<T> items = new ArrayList<>(cursor.getCount());
        try {
          while (cursor.moveToNext()) {
            items.add(mapper.apply(cursor));
          }
        } finally {
          cursor.close();
        }
        if (!isDisposed()) {
          downstream.onNext(items);
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
