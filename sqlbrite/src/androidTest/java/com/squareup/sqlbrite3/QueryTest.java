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
package com.squareup.sqlbrite3;

import android.annotation.TargetApi;
import android.arch.persistence.db.SupportSQLiteOpenHelper;
import android.arch.persistence.db.SupportSQLiteOpenHelper.Configuration;
import android.arch.persistence.db.SupportSQLiteOpenHelper.Factory;
import android.arch.persistence.db.framework.FrameworkSQLiteOpenHelperFactory;
import android.database.Cursor;
import android.os.Build;
import android.support.annotation.Nullable;
import android.support.test.InstrumentationRegistry;
import android.support.test.filters.SdkSuppress;
import com.squareup.sqlbrite3.SqlBrite.Query;
import com.squareup.sqlbrite3.TestDb.Employee;
import io.reactivex.Observable;
import io.reactivex.functions.Function;
import io.reactivex.observers.TestObserver;
import io.reactivex.schedulers.Schedulers;
import java.util.List;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;

import static com.google.common.truth.Truth.assertThat;
import static com.squareup.sqlbrite3.TestDb.Employee.MAPPER;
import static com.squareup.sqlbrite3.TestDb.SELECT_EMPLOYEES;
import static com.squareup.sqlbrite3.TestDb.TABLE_EMPLOYEE;
import static org.junit.Assert.fail;

public final class QueryTest {
  private BriteDatabase db;

  @Before public void setUp() {
    Configuration configuration = Configuration.builder(InstrumentationRegistry.getContext())
        .callback(new TestDb())
        .version(1)
        .build();

    Factory factory = new FrameworkSQLiteOpenHelperFactory();
    SupportSQLiteOpenHelper helper = factory.create(configuration);

    SqlBrite sqlBrite = new SqlBrite.Builder().build();
    db = sqlBrite.wrapDatabaseHelper(helper, Schedulers.trampoline());
  }

  @Test public void mapToOne() {
    Employee employees = db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES + " LIMIT 1")
        .lift(Query.mapToOne(MAPPER))
        .blockingFirst();
    assertThat(employees).isEqualTo(new Employee("alice", "Alice Allison"));
  }

  @Test public void mapToOneThrowsWhenMapperReturnsNull() {
    db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES + " LIMIT 1")
        .lift(Query.mapToOne(new Function<Cursor, Employee>() {
          @Override public Employee apply(Cursor cursor) throws Exception {
            return null;
          }
        }))
        .test()
        .assertError(NullPointerException.class)
        .assertErrorMessage("QueryToOne mapper returned null");
  }

  @Test public void mapToOneThrowsOnMultipleRows() {
    Observable<Employee> employees =
        db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES + " LIMIT 2") //
            .lift(Query.mapToOne(MAPPER));
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

    TestObserver<Employee> observer = new TestObserver<>();
    Observable.just(nully)
        .lift(Query.mapToOne(MAPPER))
        .subscribe(observer);

    observer.assertNoValues();
    observer.assertComplete();
  }

  @Test public void mapToOneOrDefault() {
    Employee employees = db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES + " LIMIT 1")
        .lift(Query.mapToOneOrDefault(
            MAPPER, new Employee("fred", "Fred Frederson")))
        .blockingFirst();
    assertThat(employees).isEqualTo(new Employee("alice", "Alice Allison"));
  }

  @Test public void mapToOneOrDefaultDisallowsNullDefault() {
    try {
      Query.mapToOneOrDefault(MAPPER, null);
      fail();
    } catch (NullPointerException e) {
      assertThat(e).hasMessage("defaultValue == null");
    }
  }

  @Test public void mapToOneOrDefaultThrowsWhenMapperReturnsNull() {
    db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES + " LIMIT 1")
        .lift(Query.mapToOneOrDefault(new Function<Cursor, Employee>() {
          @Override public Employee apply(Cursor cursor) throws Exception {
            return null;
          }
        }, new Employee("fred", "Fred Frederson")))
        .test()
        .assertError(NullPointerException.class)
        .assertErrorMessage("QueryToOne mapper returned null");
  }

  @Test public void mapToOneOrDefaultThrowsOnMultipleRows() {
    Observable<Employee> employees =
        db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES + " LIMIT 2") //
            .lift(Query.mapToOneOrDefault(
                MAPPER, new Employee("fred", "Fred Frederson")));
    try {
      employees.blockingFirst();
      fail();
    } catch (IllegalStateException e) {
      assertThat(e).hasMessage("Cursor returned more than 1 row");
    }
  }

  @Test public void mapToOneOrDefaultReturnsDefaultWhenNullCursor() {
    Employee defaultEmployee = new Employee("bob", "Bob Bobberson");
    Query nully = new Query() {
      @Nullable @Override public Cursor run() {
        return null;
      }
    };

    TestObserver<Employee> observer = new TestObserver<>();
    Observable.just(nully)
        .lift(Query.mapToOneOrDefault(MAPPER, defaultEmployee))
        .subscribe(observer);

    observer.assertValues(defaultEmployee);
    observer.assertComplete();
  }

  @Test public void mapToList() {
    List<Employee> employees = db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES)
        .lift(Query.mapToList(MAPPER))
        .blockingFirst();
    assertThat(employees).containsExactly( //
        new Employee("alice", "Alice Allison"), //
        new Employee("bob", "Bob Bobberson"), //
        new Employee("eve", "Eve Evenson"));
  }

  @Test public void mapToListEmptyWhenNoRows() {
    List<Employee> employees = db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES + " WHERE 1=2")
        .lift(Query.mapToList(MAPPER))
        .blockingFirst();
    assertThat(employees).isEmpty();
  }

  @Test public void mapToListReturnsNullOnMapperNull() {
    Function<Cursor, Employee> mapToNull = new Function<Cursor, Employee>() {
      private int count;

      @Override public Employee apply(Cursor cursor) throws Exception {
        return count++ == 2 ? null : MAPPER.apply(cursor);
      }
    };
    List<Employee> employees = db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES) //
            .lift(Query.mapToList(mapToNull)) //
            .blockingFirst();

    assertThat(employees).containsExactly(
        new Employee("alice", "Alice Allison"),
        new Employee("bob", "Bob Bobberson"),
        null);
  }

  @Test public void mapToListIgnoresNullCursor() {
    Query nully = new Query() {
      @Nullable @Override public Cursor run() {
        return null;
      }
    };

    TestObserver<List<Employee>> subscriber = new TestObserver<>();
    Observable.just(nully)
        .lift(Query.mapToList(MAPPER))
        .subscribe(subscriber);

    subscriber.assertNoValues();
    subscriber.assertComplete();
  }

  @SdkSuppress(minSdkVersion = Build.VERSION_CODES.N)
  @Test public void mapToOptional() {
    db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES + " LIMIT 1")
        .lift(Query.mapToOptional(MAPPER))
        .test()
        .assertValue(Optional.of(new Employee("alice", "Alice Allison")));
  }

  @SdkSuppress(minSdkVersion = Build.VERSION_CODES.N)
  @Test public void mapToOptionalThrowsWhenMapperReturnsNull() {
    db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES + " LIMIT 1")
        .lift(Query.mapToOptional(new Function<Cursor, Employee>() {
          @Override public Employee apply(Cursor cursor) throws Exception {
            return null;
          }
        }))
        .test()
        .assertError(NullPointerException.class)
        .assertErrorMessage("QueryToOne mapper returned null");
  }

  @SdkSuppress(minSdkVersion = Build.VERSION_CODES.N)
  @Test public void mapToOptionalThrowsOnMultipleRows() {
    db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES + " LIMIT 2") //
        .lift(Query.mapToOptional(MAPPER))
        .test()
        .assertError(IllegalStateException.class)
        .assertErrorMessage("Cursor returned more than 1 row");
  }

  @SdkSuppress(minSdkVersion = Build.VERSION_CODES.N)
  @Test public void mapToOptionalIgnoresNullCursor() {
    Query nully = new Query() {
      @Nullable @Override public Cursor run() {
        return null;
      }
    };

    Observable.just(nully)
        .lift(Query.mapToOptional(MAPPER))
        .test()
        .assertValue(Optional.<Employee>empty());
  }
}
