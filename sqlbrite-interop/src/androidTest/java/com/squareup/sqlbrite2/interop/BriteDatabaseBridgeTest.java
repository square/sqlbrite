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

import android.support.test.InstrumentationRegistry;
import android.support.test.runner.AndroidJUnit4;
import io.reactivex.observers.TestObserver;
import org.junit.Test;
import org.junit.runner.RunWith;
import rx.observers.AssertableSubscriber;

import static com.squareup.sqlbrite2.interop.TestDb.SELECT_EMPLOYEES;
import static com.squareup.sqlbrite2.interop.TestDb.TABLE_EMPLOYEE;
import static com.squareup.sqlbrite2.interop.TestDb.employee;
import static io.reactivex.schedulers.Schedulers.trampoline;
import static rx.schedulers.Schedulers.immediate;

@RunWith(AndroidJUnit4.class) //
public final class BriteDatabaseBridgeTest {
  private final TestDb db = new TestDb(InstrumentationRegistry.getContext(), null);

  private final com.squareup.sqlbrite.SqlBrite sqlBrite1
      = new com.squareup.sqlbrite.SqlBrite.Builder().build();
  private final com.squareup.sqlbrite2.SqlBrite sqlBrite2
      = new com.squareup.sqlbrite2.SqlBrite.Builder().build();

  private final BriteDatabaseBridge bridge =
      BriteDatabaseBridge.create(db, sqlBrite1, immediate(), sqlBrite2, trampoline());

  private final com.squareup.sqlbrite.BriteDatabase db1 = bridge.asV1();
  private final com.squareup.sqlbrite2.BriteDatabase db2 = bridge.asV2();

  @Test public void insertIntoV1TriggersV1Query() {
    AssertableSubscriber<com.squareup.sqlbrite.SqlBrite.Query> subscriber =
        db1.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).test();
    subscriber.assertValueCount(1);

    db1.insert(TABLE_EMPLOYEE, employee("Foo", "Bar"));
    subscriber.assertValueCount(2);
  }

  @Test public void insertIntoV1TriggersV2Query() {
    TestObserver<com.squareup.sqlbrite2.SqlBrite.Query> subscriber =
        db2.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).test();
    subscriber.assertValueCount(1);

    db1.insert(TABLE_EMPLOYEE, employee("Foo", "Bar"));
    subscriber.assertValueCount(2);
  }

  @Test public void insertIntoV2TriggersV1Query() {
    AssertableSubscriber<com.squareup.sqlbrite.SqlBrite.Query> subscriber =
        db1.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).test();
    subscriber.assertValueCount(1);

    db2.insert(TABLE_EMPLOYEE, employee("Foo", "Bar"));
    subscriber.assertValueCount(2);
  }

  @Test public void insertIntoV2TriggersV2Query() {
    TestObserver<com.squareup.sqlbrite2.SqlBrite.Query> subscriber =
        db2.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).test();
    subscriber.assertValueCount(1);

    db2.insert(TABLE_EMPLOYEE, employee("Foo", "Bar"));
    subscriber.assertValueCount(2);
  }
}
