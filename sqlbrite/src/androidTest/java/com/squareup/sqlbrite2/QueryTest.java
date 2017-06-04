/*
 * Copyright (C) 2015 Square, Inc.
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
import android.support.annotation.Nullable;
import android.support.test.InstrumentationRegistry;
import com.squareup.sqlbrite2.SqlBrite.Query;
import io.reactivex.Observable;
import io.reactivex.functions.Function;
import io.reactivex.observers.TestObserver;
import io.reactivex.schedulers.Schedulers;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.fail;

public final class QueryTest {
  private BriteDatabase db;

  @Before public void setUp() {
    SqlBrite sqlBrite = new SqlBrite.Builder().build();
    TestDb helper = new TestDb(InstrumentationRegistry.getContext(), null /* memory */);
    db = sqlBrite.wrapDatabaseHelper(helper, Schedulers.trampoline());
  }

  @Test public void mapToOne() {
    TestDb.Employee employees = db.createQuery(TestDb.TABLE_EMPLOYEE, TestDb.SELECT_EMPLOYEES + " LIMIT 1")
        .lift(Query.mapToOne(TestDb.Employee.MAPPER))
        .blockingFirst();
    assertThat(employees).isEqualTo(new TestDb.Employee("alice", "Alice Allison"));
  }

  @Test public void mapToOneThrowsOnMultipleRows() {
    Observable<TestDb.Employee> employees =
        db.createQuery(TestDb.TABLE_EMPLOYEE, TestDb.SELECT_EMPLOYEES + " LIMIT 2") //
            .lift(Query.mapToOne(TestDb.Employee.MAPPER));
    try {
      employees.blockingFirst();
      fail();
    } catch (IllegalStateException e) {
      assertThat(e).hasMessage("Cursor returned more than 1 row");
    }
  }

  @Test public void mapToOneIgnoresNullCursor() {
    Query nully = new Query() {
      @Nullable @Override public Cursor run() {
        return null;
      }
    };

    TestObserver<TestDb.Employee> observer = new TestObserver<>();
    Observable.just(nully)
        .lift(Query.mapToOne(TestDb.Employee.MAPPER))
        .subscribe(observer);

    observer.assertNoValues();
    observer.assertComplete();
  }

  @Test public void mapToOneOrDefault() {
    TestDb.Employee employees = db.createQuery(TestDb.TABLE_EMPLOYEE, TestDb.SELECT_EMPLOYEES + " LIMIT 1")
        .lift(Query.mapToOneOrDefault(
            TestDb.Employee.MAPPER, new TestDb.Employee("fred", "Fred Frederson")))
        .blockingFirst();
    assertThat(employees).isEqualTo(new TestDb.Employee("alice", "Alice Allison"));
  }

  @Test public void mapToOneOrDefaultDisallowsNullDefault() {
    try {
      Query.mapToOneOrDefault(TestDb.Employee.MAPPER, null);
      fail();
    } catch (NullPointerException e) {
      assertThat(e).hasMessage("defaultValue == null");
    }
  }

  @Test public void mapToOneOrDefaultThrowsOnMultipleRows() {
    Observable<TestDb.Employee> employees =
        db.createQuery(TestDb.TABLE_EMPLOYEE, TestDb.SELECT_EMPLOYEES + " LIMIT 2") //
            .lift(Query.mapToOneOrDefault(
                TestDb.Employee.MAPPER, new TestDb.Employee("fred", "Fred Frederson")));
    try {
      employees.blockingFirst();
      fail();
    } catch (IllegalStateException e) {
      assertThat(e).hasMessage("Cursor returned more than 1 row");
    }
  }

  @Test public void mapToOneOrDefaultReturnsDefaultWhenNullCursor() {
    TestDb.Employee defaultEmployee = new TestDb.Employee("bob", "Bob Bobberson");
    Query nully = new Query() {
      @Nullable @Override public Cursor run() {
        return null;
      }
    };

    TestObserver<TestDb.Employee> observer = new TestObserver<>();
    Observable.just(nully)
        .lift(Query.mapToOneOrDefault(TestDb.Employee.MAPPER, defaultEmployee))
        .subscribe(observer);

    observer.assertValues(defaultEmployee);
    observer.assertComplete();
  }

  @Test public void mapToList() {
    List<TestDb.Employee> employees = db.createQuery(TestDb.TABLE_EMPLOYEE, TestDb.SELECT_EMPLOYEES)
        .lift(Query.mapToList(TestDb.Employee.MAPPER))
        .blockingFirst();
    assertThat(employees).containsExactly( //
        new TestDb.Employee("alice", "Alice Allison"), //
        new TestDb.Employee("bob", "Bob Bobberson"), //
        new TestDb.Employee("eve", "Eve Evenson"));
  }

  @Test public void mapToListEmptyWhenNoRows() {
    List<TestDb.Employee> employees = db.createQuery(
        TestDb.TABLE_EMPLOYEE, TestDb.SELECT_EMPLOYEES + " WHERE 1=2")
        .lift(Query.mapToList(TestDb.Employee.MAPPER))
        .blockingFirst();
    assertThat(employees).isEmpty();
  }

  @Test public void mapToListReturnsNullOnMapperNull() {
    Function<Cursor, TestDb.Employee> mapToNull = new Function<Cursor, TestDb.Employee>() {
      private int count;

      @Override public TestDb.Employee apply(Cursor cursor) throws Exception {
        return count++ == 2 ? null : TestDb.Employee.MAPPER.apply(cursor);
      }
    };
    List<TestDb.Employee> employees = db.createQuery(TestDb.TABLE_EMPLOYEE, TestDb.SELECT_EMPLOYEES) //
            .lift(Query.mapToList(mapToNull)) //
            .blockingFirst();

    assertThat(employees).containsExactly(
        new TestDb.Employee("alice", "Alice Allison"),
        new TestDb.Employee("bob", "Bob Bobberson"),
        null);
  }

  @Test public void mapToListIgnoresNullCursor() {
    Query nully = new Query() {
      @Nullable @Override public Cursor run() {
        return null;
      }
    };

    TestObserver<List<TestDb.Employee>> subscriber = new TestObserver<>();
    Observable.just(nully)
        .lift(Query.mapToList(TestDb.Employee.MAPPER))
        .subscribe(subscriber);

    subscriber.assertNoValues();
    subscriber.assertComplete();
  }
}
