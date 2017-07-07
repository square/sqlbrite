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
package com.squareup.sqlbrite2.interop;

import android.database.sqlite.SQLiteOpenHelper;
import android.support.annotation.CheckResult;
import android.support.annotation.NonNull;
import com.squareup.sqlbrite.BriteDatabaseV1Factory;
import com.squareup.sqlbrite2.BriteDatabaseV2Factory;
import io.reactivex.disposables.Disposable;
import java.util.Set;

/**
 * A bridge between SQL Brite 1.x and 2.x allowing triggers from mutations in 1.x to trigger 2.x
 * queries and vise versa.
 * <p>
 * Create a 1.x and 2.x instance of {@code SqlBrite}. Pass to {@link #create} with your schedulers
 * and {@link SQLiteOpenHelper} to create an instance of this class with both 1.x and 2.x triggers
 * connected. Then access the 1.x and 2.x instances of {@code BriteDatabase} via {@link #asV1()}
 * and {@link #asV2()}.
 */
public final class BriteDatabaseBridge {
  @SuppressWarnings("ConstantConditions") // Public API contract validation.
  @CheckResult @NonNull
  public static BriteDatabaseBridge create(@NonNull SQLiteOpenHelper helper,
      @NonNull com.squareup.sqlbrite.SqlBrite sqlBrite1, @NonNull rx.Scheduler scheduler1,
      @NonNull com.squareup.sqlbrite2.SqlBrite sqlBrite2,
      @NonNull io.reactivex.Scheduler scheduler2) {
    if (helper == null) throw new NullPointerException("helper == null");
    if (sqlBrite1 == null) throw new NullPointerException("sqlBrite1 == null");
    if (scheduler1 == null) throw new NullPointerException("scheduler1 == null");
    if (sqlBrite2 == null) throw new NullPointerException("sqlBrite2 == null");
    if (scheduler2 == null) throw new NullPointerException("scheduler2 == null");
    return new BriteDatabaseBridge(helper, sqlBrite1, scheduler1, sqlBrite2, scheduler2);
  }

  private final com.squareup.sqlbrite.BriteDatabase database1;
  private final com.squareup.sqlbrite2.BriteDatabase database2;

  private BriteDatabaseBridge(SQLiteOpenHelper helper,
      com.squareup.sqlbrite.SqlBrite sqlBrite1, rx.Scheduler scheduler1,
      com.squareup.sqlbrite2.SqlBrite sqlBrite2, io.reactivex.Scheduler scheduler2) {
    io.reactivex.subjects.PublishSubject<Set<String>> source2 =
        io.reactivex.subjects.PublishSubject.create();
    rx.subjects.PublishSubject<Set<String>> source1 =
        rx.subjects.PublishSubject.create();
    MultiSubjectDispatcher<Set<String>> sink = new MultiSubjectDispatcher<>(source1, source2);
    database1 = BriteDatabaseV1Factory.create(sqlBrite1, helper, source1, sink, scheduler1);
    database2 = BriteDatabaseV2Factory.create(sqlBrite2, helper, source2, sink, scheduler2);
  }

  @CheckResult @NonNull
  public com.squareup.sqlbrite.BriteDatabase asV1() {
    return database1;
  }

  @CheckResult @NonNull
  public com.squareup.sqlbrite2.BriteDatabase asV2() {
    return database2;
  }

  static class MultiSubjectDispatcher<T> implements rx.Observer<T>, io.reactivex.Observer<T> {
    private final rx.subjects.PublishSubject<T> subject1;
    private final io.reactivex.subjects.PublishSubject<T> subject2;

    MultiSubjectDispatcher(rx.subjects.PublishSubject<T> subject1,
        io.reactivex.subjects.PublishSubject<T> subject2) {
      this.subject1 = subject1;
      this.subject2 = subject2;
    }

    @Override public void onNext(T value) {
      subject1.onNext(value);
      subject2.onNext(value);
    }

    @Override public void onSubscribe(Disposable d) {}
    @Override public void onComplete() {}
    @Override public void onCompleted() {}
    @Override public void onError(Throwable e) {}
  }
}
