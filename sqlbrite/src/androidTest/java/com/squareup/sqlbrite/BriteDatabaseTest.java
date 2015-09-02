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

import android.content.ContentValues;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteException;
import android.support.test.InstrumentationRegistry;
import android.support.test.runner.AndroidJUnit4;
import com.google.common.collect.Range;
import com.squareup.sqlbrite.BriteDatabase.Transaction;
import com.squareup.sqlbrite.TestDb.Employee;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import rx.Observable;
import rx.Subscription;
import rx.functions.Func1;
import rx.observables.BlockingObservable;

import static android.database.sqlite.SQLiteDatabase.CONFLICT_IGNORE;
import static com.google.common.truth.Truth.assertThat;
import static com.squareup.sqlbrite.RecordingObserver.CursorAssert;
import static com.squareup.sqlbrite.SqlBrite.Query;
import static com.squareup.sqlbrite.TestDb.EmployeeTable.ID;
import static com.squareup.sqlbrite.TestDb.EmployeeTable.NAME;
import static com.squareup.sqlbrite.TestDb.EmployeeTable.USERNAME;
import static com.squareup.sqlbrite.TestDb.ManagerTable.EMPLOYEE_ID;
import static com.squareup.sqlbrite.TestDb.ManagerTable.MANAGER_ID;
import static com.squareup.sqlbrite.TestDb.TABLE_EMPLOYEE;
import static com.squareup.sqlbrite.TestDb.TABLE_MANAGER;
import static com.squareup.sqlbrite.TestDb.employee;
import static com.squareup.sqlbrite.TestDb.manager;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.fail;

@RunWith(AndroidJUnit4.class)
public final class BriteDatabaseTest {
  private static final Collection<String> BOTH_TABLES =
      Arrays.asList(TABLE_EMPLOYEE, TABLE_MANAGER);
  private static final String SELECT_EMPLOYEES =
      "SELECT " + USERNAME + ", " + NAME + " FROM " + TABLE_EMPLOYEE;
  private static final String SELECT_MANAGER_LIST = ""
      + "SELECT e." + NAME + ", m." + NAME + " "
      + "FROM " + TABLE_MANAGER + " AS manager "
      + "JOIN " + TABLE_EMPLOYEE + " AS e "
      + "ON manager." + EMPLOYEE_ID + " = e." + ID + " "
      + "JOIN " + TABLE_EMPLOYEE + " as m "
      + "ON manager." + MANAGER_ID + " = m." + ID;

  private final List<String> logs = new ArrayList<>();
  private final RecordingObserver o = new RecordingObserver();

  private TestDb helper;
  private SQLiteDatabase real;
  private BriteDatabase db;

  @Before public void setUp() {
    helper = new TestDb(InstrumentationRegistry.getContext());
    real = helper.getWritableDatabase();

    SqlBrite.Logger logger = new SqlBrite.Logger() {
      @Override public void log(String message) {
        logs.add(message);
      }
    };
    db = new BriteDatabase(helper, logger);
  }

  @After public void tearDown() {
    o.assertNoMoreEvents();
  }

  @Test public void loggerEnabled() {
    db.setLoggingEnabled(true);
    db.insert(TABLE_EMPLOYEE, employee("john", "John Johnson"));
    assertThat(logs).isNotEmpty();
  }

  @Test public void loggerDisabled() {
    db.setLoggingEnabled(false);
    db.insert(TABLE_EMPLOYEE, employee("john", "John Johnson"));
    assertThat(logs).isEmpty();
  }

  @Test public void closePropagates() throws IOException {
    db.close();
    assertThat(real.isOpen()).isFalse();
  }

  @Test public void query() {
    db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();
  }

  @Test public void queryMapToList() {
    List<Employee> employees = db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES)
        .mapToList(Employee.MAPPER)
        .toBlocking()
        .first();
    assertThat(employees).containsExactly( //
        new Employee("alice", "Alice Allison"), //
        new Employee("bob", "Bob Bobberson"), //
        new Employee("eve", "Eve Evenson"));
  }

  @Test public void queryMapToListEmpty() {
    List<Employee> employees = db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES + " WHERE 1=2")
        .mapToList(Employee.MAPPER)
        .toBlocking()
        .first();
    assertThat(employees).isEmpty();
  }

  @Test public void queryMapToListMapperReturnNullThrows() {
    BlockingObservable<List<Employee>> employees =
        db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES) //
            .mapToList(new Func1<Cursor, Employee>() {
              private int count;

              @Override public Employee call(Cursor cursor) {
                return count++ == 2 ? null : Employee.MAPPER.call(cursor);
              }
            }) //
            .toBlocking();
    try {
      employees.first();
    } catch (NullPointerException e) {
      assertThat(e).hasMessage("Mapper returned null for row 3");
      assertThat(e.getCause()).hasMessage(
          "OnError while emitting onNext value: SELECT username, name FROM employee");
    }
  }

  @Test public void queryMapToOne() {
    Employee employees = db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES + " LIMIT 1")
        .mapToOne(Employee.MAPPER)
        .toBlocking()
        .first();
    assertThat(employees).isEqualTo(new Employee("alice", "Alice Allison"));
  }

  @Ignore("How to test in black box way? Can't take(1).mapToOne() to trigger complete.") // TODO
  @Test public void queryMapToOneEmpty() {
    db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES + " WHERE 1=2")
        .mapToOne(Employee.MAPPER)
        .toBlocking()
        .first();
  }

  @Test public void queryMapToOneMapperReturnNullThrows() {
    BlockingObservable<Employee> employees =
        db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES) //
            .mapToOne(new Func1<Cursor, Employee>() {
              @Override public Employee call(Cursor cursor) {
                return null;
              }
            }) //
            .toBlocking();
    try {
      employees.first();
    } catch (NullPointerException e) {
      assertThat(e).hasMessage("Mapper returned null for row 1");
      assertThat(e.getCause()).hasMessage(
          "OnError while emitting onNext value: SELECT username, name FROM employee");
    }
  }

  @Test public void queryMapToOneMultipleRowsThrows() {
    BlockingObservable<Employee> employees =
        db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES + " LIMIT 2") //
            .mapToOne(Employee.MAPPER) //
            .toBlocking();
    try {
      employees.first();
    } catch (IllegalStateException e) {
      assertThat(e).hasMessage("Cursor returned more than 1 row");
      assertThat(e.getCause()).hasMessage(
          "OnError while emitting onNext value: SELECT username, name FROM employee LIMIT 2");
    }
  }

  @Test public void queryMapToOneOrDefault() {
    Employee employees = db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES + " LIMIT 1")
        .mapToOneOrDefault(Employee.MAPPER, null)
        .toBlocking()
        .first();
    assertThat(employees).isEqualTo(new Employee("alice", "Alice Allison"));
  }

  @Test public void queryMapToOneOrDefaultEmpty() {
    Employee employees = db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES + " WHERE 1=2")
        .mapToOneOrDefault(Employee.MAPPER, new Employee("bob", "Bob Bobberson"))
        .toBlocking()
        .first();
    assertThat(employees).isEqualTo(new Employee("bob", "Bob Bobberson"));
  }

  @Test public void queryMapToOneOrDefaultEmptyUsingNull() {
    Employee employees = db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES + " WHERE 1=2")
        .mapToOneOrDefault(Employee.MAPPER, null)
        .toBlocking()
        .first();
    assertThat(employees).isNull();
  }

  @Test public void queryMapToOneOrDefaultMapperReturnNullThrows() {
    BlockingObservable<Employee> employees =
        db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES) //
            .mapToOneOrDefault(new Func1<Cursor, Employee>() {
              @Override public Employee call(Cursor cursor) {
                return null;
              }
            }, null) //
            .toBlocking();
    try {
      employees.first();
    } catch (NullPointerException e) {
      assertThat(e).hasMessage("Mapper returned null for row 1");
      assertThat(e.getCause()).hasMessage(
          "OnError while emitting onNext value: SELECT username, name FROM employee");
    }
  }

  @Test public void queryMapToOneOrDefaultMultipleRowsThrows() {
    BlockingObservable<Employee> employees =
        db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES + " LIMIT 2") //
            .mapToOneOrDefault(Employee.MAPPER, null) //
            .toBlocking();
    try {
      employees.first();
    } catch (IllegalStateException e) {
      assertThat(e).hasMessage("Cursor returned more than 1 row");
      assertThat(e.getCause()).hasMessage(
          "OnError while emitting onNext value: SELECT username, name FROM employee LIMIT 2");
    }
  }

  @Test public void badQueryCallsError() {
    db.createQuery(TABLE_EMPLOYEE, "SELECT * FROM missing").subscribe(o);
    o.assertErrorContains("no such table: missing");
  }

  @Test public void queryWithArgs() {
    db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES + " WHERE " + USERNAME + " = ?", "bob")
        .subscribe(o);
    o.assertCursor()
        .hasRow("bob", "Bob Bobberson")
        .isExhausted();
  }

  @Test public void queryObservesInsert() {
    db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();

    db.insert(TABLE_EMPLOYEE, employee("john", "John Johnson"));
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .hasRow("john", "John Johnson")
        .isExhausted();
  }

  @Test public void queryObservesInsertDebounced() {
    db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES)
        .debounce(500, MILLISECONDS)
        .subscribe(o);
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();

    long startNs = System.nanoTime();

    // Shotgun 10 inserts which will cause 10 triggers. DO NOT DO THIS IRL! Use a transaction!
    for (int i = 0; i < 10; i++) {
      db.insert(TABLE_EMPLOYEE, employee("john" + i, "John Johnson " + i));
    }

    // Only one trigger should have been observed.
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .hasRow("john0", "John Johnson 0")
        .hasRow("john1", "John Johnson 1")
        .hasRow("john2", "John Johnson 2")
        .hasRow("john3", "John Johnson 3")
        .hasRow("john4", "John Johnson 4")
        .hasRow("john5", "John Johnson 5")
        .hasRow("john6", "John Johnson 6")
        .hasRow("john7", "John Johnson 7")
        .hasRow("john8", "John Johnson 8")
        .hasRow("john9", "John Johnson 9")
        .isExhausted();

    long tookNs = System.nanoTime() - startNs;
    assertThat(TimeUnit.NANOSECONDS.toMillis(tookNs)).isIn(Range.atLeast(500L));
  }

  @Test public void queryNotNotifiedWhenInsertFails() {
    db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();

    db.insert(TABLE_EMPLOYEE, employee("bob", "Bob Bobberson"), CONFLICT_IGNORE);
    o.assertNoMoreEvents();
  }

  @Test public void queryObservesUpdate() {
    db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();

    ContentValues values = new ContentValues();
    values.put(NAME, "Robert Bobberson");
    db.update(TABLE_EMPLOYEE, values, USERNAME + " = 'bob'");
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Robert Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();
  }

  @Test public void queryNotNotifiedWhenUpdateAffectsZeroRows() {
    db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();

    ContentValues values = new ContentValues();
    values.put(NAME, "John Johnson");
    db.update(TABLE_EMPLOYEE, values, USERNAME + " = 'john'");
    o.assertNoMoreEvents();
  }

  @Test public void queryObservesDelete() {
    db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();

    db.delete(TABLE_EMPLOYEE, USERNAME + " = 'bob'");
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();
  }

  @Test public void queryNotNotifiedWhenDeleteAffectsZeroRows() {
    db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();

    db.delete(TABLE_EMPLOYEE, USERNAME + " = 'john'");
    o.assertNoMoreEvents();
  }

  @Test public void queryMultipleTables() {
    db.createQuery(BOTH_TABLES, SELECT_MANAGER_LIST).subscribe(o);
    o.assertCursor()
        .hasRow("Eve Evenson", "Alice Allison")
        .isExhausted();
  }

  @Test public void queryMultipleTablesObservesChanges() {
    db.createQuery(BOTH_TABLES, SELECT_MANAGER_LIST).subscribe(o);
    o.assertCursor()
        .hasRow("Eve Evenson", "Alice Allison")
        .isExhausted();

    // A new employee triggers, despite the fact that it's not in our result set.
    db.insert(TABLE_EMPLOYEE, employee("john", "John Johnson"));
    o.assertCursor()
        .hasRow("Eve Evenson", "Alice Allison")
        .isExhausted();

    // A new manager also triggers and it is in our result set.
    db.insert(TABLE_MANAGER, manager(helper.bobId, helper.eveId));
    o.assertCursor()
        .hasRow("Eve Evenson", "Alice Allison")
        .hasRow("Bob Bobberson", "Eve Evenson")
        .isExhausted();
  }

  @Test public void queryMultipleTablesObservesChangesOnlyOnce() {
    // Employee table is in this list twice. We should still only be notified once for a change.
    List<String> tables = Arrays.asList(TABLE_EMPLOYEE, TABLE_MANAGER, TABLE_EMPLOYEE);
    db.createQuery(tables, SELECT_MANAGER_LIST).subscribe(o);
    o.assertCursor()
        .hasRow("Eve Evenson", "Alice Allison")
        .isExhausted();

    ContentValues values = new ContentValues();
    values.put(NAME, "Even Evenson");
    db.update(TABLE_EMPLOYEE, values, USERNAME + " = 'eve'");
    o.assertCursor()
        .hasRow("Even Evenson", "Alice Allison")
        .isExhausted();
  }

  @Test public void queryNotNotifiedAfterUnsubscribe() {
    Subscription subscription = db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();
    subscription.unsubscribe();

    db.insert(TABLE_EMPLOYEE, employee("john", "John Johnson"));
    o.assertNoMoreEvents();
  }

  @Test public void queryOnlyNotifiedAfterSubscribe() {
    Observable<Query> query = db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES);
    o.assertNoMoreEvents();

    db.insert(TABLE_EMPLOYEE, employee("john", "John Johnson"));
    o.assertNoMoreEvents();

    query.subscribe(o);
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .hasRow("john", "John Johnson")
        .isExhausted();
  }

  @Test public void transactionOnlyNotifiesOnce() {
    db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();

    Transaction transaction = db.newTransaction();
    try {
      db.insert(TABLE_EMPLOYEE, employee("john", "John Johnson"));
      db.insert(TABLE_EMPLOYEE, employee("nick", "Nick Nickers"));
      o.assertNoMoreEvents();

      transaction.markSuccessful();
    } finally {
      transaction.end();
    }

    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .hasRow("john", "John Johnson")
        .hasRow("nick", "Nick Nickers")
        .isExhausted();
  }

  @Test public void transactionIsCloseable() throws IOException {
    db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();

    Transaction transaction = db.newTransaction();
    //noinspection UnnecessaryLocalVariable
    Closeable closeableTransaction = transaction; // Verify type is implemented.
    try {
      db.insert(TABLE_EMPLOYEE, employee("john", "John Johnson"));
      db.insert(TABLE_EMPLOYEE, employee("nick", "Nick Nickers"));
      transaction.markSuccessful();
    } finally {
      closeableTransaction.close();
    }

    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .hasRow("john", "John Johnson")
        .hasRow("nick", "Nick Nickers")
        .isExhausted();
  }

  @Test public void transactionDoesNotThrow() {
    db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();

    Transaction transaction = db.newTransaction();
    try {
      db.insert(TABLE_EMPLOYEE, employee("john", "John Johnson"));
      db.insert(TABLE_EMPLOYEE, employee("nick", "Nick Nickers"));
      transaction.markSuccessful();
    } finally {
      transaction.close(); // Transactions should not throw on close().
    }

    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .hasRow("john", "John Johnson")
        .hasRow("nick", "Nick Nickers")
        .isExhausted();
  }

  @Test public void queryCreatedDuringTransactionThrows() {
    db.newTransaction();
    try {
      db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES);
      fail();
    } catch (IllegalStateException e) {
      assertThat(e.getMessage()).startsWith("Cannot create observable query in transaction.");
    }
  }

  @Test public void querySubscribedToDuringTransactionThrows() {
    Observable<Query> query = db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES);

    db.newTransaction();
    query.subscribe(o);
    o.assertErrorContains("Cannot subscribe to observable query in a transaction.");
  }

  @Test public void callingEndMultipleTimesThrows() {
    Transaction transaction = db.newTransaction();
    transaction.end();
    try {
      transaction.end();
      fail();
    } catch (IllegalStateException e) {
      assertThat(e).hasMessage("Not in transaction.");
    }
  }

  @Test public void querySubscribedToDuringTransactionOnDifferentThread()
      throws InterruptedException {
    Transaction transaction = db.newTransaction();

    final CountDownLatch latch = new CountDownLatch(1);
    new Thread() {
      @Override public void run() {
        db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
        latch.countDown();
      }
    }.start();

    Thread.sleep(500); // Wait for the thread to block on initial query.
    o.assertNoMoreEvents();

    transaction.end(); // Allow other queries to continue.
    latch.await(500, MILLISECONDS); // Wait for thread to observe initial query.

    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();
  }

  @Test public void queryCreatedBeforeTransactionButSubscribedAfter() {
    Observable<Query> query = db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES);

    Transaction transaction = db.newTransaction();
    try {
      db.insert(TABLE_EMPLOYEE, employee("john", "John Johnson"));
      db.insert(TABLE_EMPLOYEE, employee("nick", "Nick Nickers"));
      transaction.markSuccessful();
    } finally {
      transaction.end();
    }

    query.subscribe(o);
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .hasRow("john", "John Johnson")
        .hasRow("nick", "Nick Nickers")
        .isExhausted();
  }

  @Test public void synchronousQueryDuringTransaction() {
    Transaction transaction = db.newTransaction();
    try {
      transaction.markSuccessful();
      assertCursor(db.query(SELECT_EMPLOYEES))
          .hasRow("alice", "Alice Allison")
          .hasRow("bob", "Bob Bobberson")
          .hasRow("eve", "Eve Evenson")
          .isExhausted();
    } finally {
      transaction.end();
    }
  }

  @Test public void synchronousQueryDuringTransactionSeesChanges() {
    Transaction transaction = db.newTransaction();
    try {
      assertCursor(db.query(SELECT_EMPLOYEES))
          .hasRow("alice", "Alice Allison")
          .hasRow("bob", "Bob Bobberson")
          .hasRow("eve", "Eve Evenson")
          .isExhausted();

      db.insert(TABLE_EMPLOYEE, employee("john", "John Johnson"));
      db.insert(TABLE_EMPLOYEE, employee("nick", "Nick Nickers"));

      assertCursor(db.query(SELECT_EMPLOYEES))
          .hasRow("alice", "Alice Allison")
          .hasRow("bob", "Bob Bobberson")
          .hasRow("eve", "Eve Evenson")
          .hasRow("john", "John Johnson")
          .hasRow("nick", "Nick Nickers")
          .isExhausted();

      transaction.markSuccessful();
    } finally {
      transaction.end();
    }
  }

  @Test public void nestedTransactionsOnlyNotifyOnce() {
    db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();

    Transaction transactionOuter = db.newTransaction();
    try {
      db.insert(TABLE_EMPLOYEE, employee("john", "John Johnson"));

      Transaction transactionInner = db.newTransaction();
      try {
        db.insert(TABLE_EMPLOYEE, employee("nick", "Nick Nickers"));
        transactionInner.markSuccessful();
      } finally {
        transactionInner.end();
      }

      transactionOuter.markSuccessful();
    } finally {
      transactionOuter.end();
    }

    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .hasRow("john", "John Johnson")
        .hasRow("nick", "Nick Nickers")
        .isExhausted();
  }

  @Test public void nestedTransactionsOnMultipleTables() {
    db.createQuery(BOTH_TABLES, SELECT_MANAGER_LIST).subscribe(o);
    o.assertCursor()
        .hasRow("Eve Evenson", "Alice Allison")
        .isExhausted();

    Transaction transactionOuter = db.newTransaction();
    try {

      Transaction transactionInner = db.newTransaction();
      try {
        db.insert(TABLE_EMPLOYEE, employee("john", "John Johnson"));
        transactionInner.markSuccessful();
      } finally {
        transactionInner.end();
      }

      transactionInner = db.newTransaction();
      try {
        db.insert(TABLE_MANAGER, manager(helper.aliceId, helper.bobId));
        transactionInner.markSuccessful();
      } finally {
        transactionInner.end();
      }

      transactionOuter.markSuccessful();
    } finally {
      transactionOuter.end();
    }

    o.assertCursor()
        .hasRow("Eve Evenson", "Alice Allison")
        .hasRow("Alice Allison", "Bob Bobberson")
        .isExhausted();
  }

  @Test public void emptyTransactionDoesNotNotify() {
    db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();

    Transaction transaction = db.newTransaction();
    try {
      transaction.markSuccessful();
    } finally {
      transaction.end();
    }
    o.assertNoMoreEvents();
  }

  @Test public void transactionRollbackDoesNotNotify() {
    db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();

    Transaction transaction = db.newTransaction();
    try {
      db.insert(TABLE_EMPLOYEE, employee("john", "John Johnson"));
      db.insert(TABLE_EMPLOYEE, employee("nick", "Nick Nickers"));
      // No call to set successful.
    } finally {
      transaction.end();
    }
    o.assertNoMoreEvents();
  }

  @Test public void backpressureSupported() {
    o.doRequest(2);

    db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();

    db.insert(TABLE_EMPLOYEE, employee("john", "John Johnson"));
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .hasRow("john", "John Johnson")
        .isExhausted();

    db.insert(TABLE_EMPLOYEE, employee("nick", "Nick Nickers"));
    o.assertNoMoreEvents();

    db.delete(TABLE_EMPLOYEE, USERNAME + "=?", "bob");
    o.assertNoMoreEvents();

    o.doRequest(1);
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("eve", "Eve Evenson")
        .hasRow("john", "John Johnson")
        .hasRow("nick", "Nick Nickers")
        .isExhausted();

    db.delete(TABLE_EMPLOYEE, USERNAME + "=?", "eve");
    o.assertNoMoreEvents();
    db.delete(TABLE_EMPLOYEE, USERNAME + "=?", "alice");
    o.assertNoMoreEvents();

    o.doRequest(Long.MAX_VALUE);
    o.assertCursor()
        .hasRow("john", "John Johnson")
        .hasRow("nick", "Nick Nickers")
        .isExhausted();
    o.assertNoMoreEvents();
  }

  @Test public void badQueryThrows() {
    try {
      db.query("SELECT * FROM missing");
      fail();
    } catch (SQLiteException e) {
      assertThat(e.getMessage()).contains("no such table: missing");
    }
  }

  @Test public void badInsertThrows() {
    try {
      db.insert("missing", employee("john", "John Johnson"));
      fail();
    } catch (SQLiteException e) {
      assertThat(e.getMessage()).contains("no such table: missing");
    }
  }

  @Test public void badUpdateThrows() {
    try {
      db.update("missing", employee("john", "John Johnson"), "1");
      fail();
    } catch (SQLiteException e) {
      assertThat(e.getMessage()).contains("no such table: missing");
    }
  }

  @Test public void badDeleteThrows() {
    try {
      db.delete("missing", "1");
      fail();
    } catch (SQLiteException e) {
      assertThat(e.getMessage()).contains("no such table: missing");
    }
  }

  private static CursorAssert assertCursor(Cursor cursor) {
    return new CursorAssert(cursor);
  }
}
