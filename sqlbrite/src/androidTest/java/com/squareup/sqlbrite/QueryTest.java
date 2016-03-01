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
package com.squareup.sqlbrite;

import android.database.Cursor;
import android.support.annotation.Nullable;
import android.support.test.InstrumentationRegistry;
import com.squareup.sqlbrite.SqlBrite.Query;
import com.squareup.sqlbrite.TestDb.Employee;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import rx.Observable;
import rx.functions.Func1;
import rx.observables.BlockingObservable;
import rx.observers.TestSubscriber;
import rx.schedulers.Schedulers;

import static com.google.common.truth.Truth.assertThat;
import static com.squareup.sqlbrite.TestDb.SELECT_EMPLOYEES;
import static com.squareup.sqlbrite.TestDb.TABLE_EMPLOYEE;

public final class QueryTest {
  private BriteDatabase db;

  @Before public void setUp() {
    SqlBrite sqlBrite = SqlBrite.create();
    TestDb helper = new TestDb(InstrumentationRegistry.getContext());
    db = sqlBrite.wrapDatabaseHelper(helper, Schedulers.immediate());
  }

  @Test public void mapToOne() {
    Employee employees = db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES + " LIMIT 1")
        .lift(Query.mapToOne(Employee.MAPPER))
        .toBlocking()
        .first();
    assertThat(employees).isEqualTo(new Employee("alice", "Alice Allison"));
  }

  @Test public void mapToOneThrowsOnMapperNull() {
    BlockingObservable<Employee> employees =
        db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES) //
            .lift(Query.mapToOne(new Func1<Cursor, Employee>() {
              @Override public Employee call(Cursor cursor) {
                return null;
              }
            })) //
            .toBlocking();
    try {
      employees.first();
    } catch (NullPointerException e) {
      assertThat(e).hasMessage("Mapper returned null for row 1");
      assertThat(e.getCause()).hasMessage(
          "OnError while emitting onNext value: SELECT username, name FROM employee");
    }
  }

  @Test public void mapToOneNoOpOnNoRows() {
    List<Employee> employees = db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES + " WHERE 1=2")
        .take(1)
        .lift(Query.mapToOne(Employee.MAPPER))
        .toList()
        .toBlocking()
        .first();
    assertThat(employees).isEmpty();
  }

  @Test public void mapToOneThrowsOnMultipleRows() {
    BlockingObservable<Employee> employees =
        db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES + " LIMIT 2") //
            .lift(Query.mapToOne(Employee.MAPPER)) //
            .toBlocking();
    try {
      employees.first();
    } catch (IllegalStateException e) {
      assertThat(e).hasMessage("Cursor returned more than 1 row");
      assertThat(e.getCause()).hasMessage(
          "OnError while emitting onNext value: SELECT username, name FROM employee LIMIT 2");
    }
  }

  @Test public void mapToOneIgnoresNullCursor() {
    Query nully = new Query() {
      @Nullable @Override public Cursor run() {
        return null;
      }
    };

    TestSubscriber<Employee> subscriber = new TestSubscriber<>();
    Observable.just(nully)
        .lift(Query.mapToOne(Employee.MAPPER))
        .subscribe(subscriber);

    subscriber.assertNoValues();
    subscriber.assertCompleted();
  }

  @Test public void mapToOneOrDefault() {
    Employee employees = db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES + " LIMIT 1")
        .lift(Query.mapToOneOrDefault(Employee.MAPPER, null))
        .toBlocking()
        .first();
    assertThat(employees).isEqualTo(new Employee("alice", "Alice Allison"));
  }

  @Test public void mapToOneOrDefaultReturnsDefaultWhenNoRows() {
    Employee defaultEmployee = new Employee("bob", "Bob Bobberson");
    Employee employees = db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES + " WHERE 1=2")
        .lift(Query.mapToOneOrDefault(Employee.MAPPER, defaultEmployee))
        .toBlocking()
        .first();
    assertThat(employees).isSameAs(defaultEmployee);
  }

  @Test public void mapToOneOrDefaultAllowsNullDefault() {
    Employee employees = db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES + " WHERE 1=2")
        .lift(Query.mapToOneOrDefault(Employee.MAPPER, null))
        .toBlocking()
        .first();
    assertThat(employees).isNull();
  }

  @Test public void mapToOneOrDefaultThrowsOnMapperNull() {
    BlockingObservable<Employee> employees =
        db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES) //
            .lift(Query.mapToOneOrDefault(new Func1<Cursor, Employee>() {
              @Override public Employee call(Cursor cursor) {
                return null;
              }
            }, null)) //
            .toBlocking();
    try {
      employees.first();
    } catch (NullPointerException e) {
      assertThat(e).hasMessage("Mapper returned null for row 1");
      assertThat(e.getCause()).hasMessage(
          "OnError while emitting onNext value: SELECT username, name FROM employee");
    }
  }

  @Test public void mapToOneOrDefaultThrowsOnMultipleRows() {
    BlockingObservable<Employee> employees =
        db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES + " LIMIT 2") //
            .lift(Query.mapToOneOrDefault(Employee.MAPPER, null)) //
            .toBlocking();
    try {
      employees.first();
    } catch (IllegalStateException e) {
      assertThat(e).hasMessage("Cursor returned more than 1 row");
      assertThat(e.getCause()).hasMessage(
          "OnError while emitting onNext value: SELECT username, name FROM employee LIMIT 2");
    }
  }

  @Test public void mapToOneOrDefaultReturnsDefaultWhenNullCursor() {
    Employee defaultEmployee = new Employee("bob", "Bob Bobberson");
    Query nully = new Query() {
      @Nullable @Override public Cursor run() {
        return null;
      }
    };

    TestSubscriber<Employee> subscriber = new TestSubscriber<>();
    Observable.just(nully)
        .lift(Query.mapToOneOrDefault(Employee.MAPPER, defaultEmployee))
        .subscribe(subscriber);

    subscriber.assertValues(defaultEmployee);
    subscriber.assertCompleted();
  }

  @Test public void mapToList() {
    List<Employee> employees = db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES)
        .lift(Query.mapToList(Employee.MAPPER))
        .toBlocking()
        .first();
    assertThat(employees).containsExactly( //
        new Employee("alice", "Alice Allison"), //
        new Employee("bob", "Bob Bobberson"), //
        new Employee("eve", "Eve Evenson"));
  }

  @Test public void mapToListEmptyWhenNoRows() {
    List<Employee> employees = db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES + " WHERE 1=2")
        .lift(Query.mapToList(Employee.MAPPER))
        .toBlocking()
        .first();
    assertThat(employees).isEmpty();
  }

  @Test public void mapToListThrowsOnMapperNull() {
    BlockingObservable<List<Employee>> employees =
        db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES) //
            .lift(Query.mapToList(new Func1<Cursor, Employee>() {
              private int count;

              @Override public Employee call(Cursor cursor) {
                return count++ == 2 ? null : Employee.MAPPER.call(cursor);
              }
            })) //
            .toBlocking();
    try {
      employees.first();
    } catch (NullPointerException e) {
      assertThat(e).hasMessage("Mapper returned null for row 3");
      assertThat(e.getCause()).hasMessage(
          "OnError while emitting onNext value: SELECT username, name FROM employee");
    }
  }

  @Test public void mapToListIgnoresNullCursor() {
    Query nully = new Query() {
      @Nullable @Override public Cursor run() {
        return null;
      }
    };

    TestSubscriber<List<Employee>> subscriber = new TestSubscriber<>();
    Observable.just(nully)
        .lift(Query.mapToList(Employee.MAPPER))
        .subscribe(subscriber);

    subscriber.assertNoValues();
    subscriber.assertCompleted();
  }
}
