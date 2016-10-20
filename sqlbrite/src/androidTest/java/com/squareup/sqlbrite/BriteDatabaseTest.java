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

import android.annotation.TargetApi;
import android.content.ContentValues;
import android.database.Cursor;
import android.database.SQLException;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteException;
import android.database.sqlite.SQLiteStatement;
import android.os.Build;
import android.support.test.InstrumentationRegistry;
import android.support.test.filters.SdkSuppress;
import android.support.test.runner.AndroidJUnit4;
import com.squareup.sqlbrite.BriteDatabase.Transaction;
import com.squareup.sqlbrite.TestDb.Employee;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import rx.Observable;
import rx.Observable.Transformer;
import rx.Subscription;
import rx.functions.Action1;
import rx.internal.util.RxRingBuffer;
import rx.subjects.PublishSubject;

import static android.database.sqlite.SQLiteDatabase.CONFLICT_IGNORE;
import static com.google.common.truth.Truth.assertThat;
import static com.squareup.sqlbrite.RecordingObserver.CursorAssert;
import static com.squareup.sqlbrite.SqlBrite.Query;
import static com.squareup.sqlbrite.TestDb.BOTH_TABLES;
import static com.squareup.sqlbrite.TestDb.EmployeeTable.NAME;
import static com.squareup.sqlbrite.TestDb.EmployeeTable.USERNAME;
import static com.squareup.sqlbrite.TestDb.SELECT_EMPLOYEES;
import static com.squareup.sqlbrite.TestDb.SELECT_MANAGER_LIST;
import static com.squareup.sqlbrite.TestDb.TABLE_EMPLOYEE;
import static com.squareup.sqlbrite.TestDb.TABLE_MANAGER;
import static com.squareup.sqlbrite.TestDb.employee;
import static com.squareup.sqlbrite.TestDb.manager;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.fail;

@RunWith(AndroidJUnit4.class)
public final class BriteDatabaseTest {
  private final List<String> logs = new ArrayList<>();
  private final RecordingObserver o = new RecordingObserver();
  private final TestScheduler scheduler = new TestScheduler();
  private final PublishSubject<Void> killSwitch = PublishSubject.create();

  private TestDb helper;
  private SQLiteDatabase real;
  private BriteDatabase db;

  @Rule
  public TemporaryFolder dbFolder = new TemporaryFolder();

  @Before public void setUp() throws IOException {
    helper = new TestDb(InstrumentationRegistry.getContext(), dbFolder.newFile().getPath());
    real = helper.getWritableDatabase();

    SqlBrite.Logger logger = new SqlBrite.Logger() {
      @Override public void log(String message) {
        logs.add(message);
      }
    };
    Transformer<Query, Query> queryTransformer = new Transformer<Query, Query>() {
      @Override public Observable<Query> call(Observable<Query> queryObservable) {
        return queryObservable.takeUntil(killSwitch);
      }
    };
    db = new BriteDatabase(helper, logger, scheduler, queryTransformer);
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

  @Test public void closePropagates() {
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

  @Test public void queryMapToOne() {
    Employee employees = db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES + " LIMIT 1")
        .mapToOne(Employee.MAPPER)
        .toBlocking()
        .first();
    assertThat(employees).isEqualTo(new Employee("alice", "Alice Allison"));
  }

  @Test public void queryMapToOneOrDefault() {
    Employee employees = db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES + " LIMIT 1")
        .mapToOneOrDefault(Employee.MAPPER, null)
        .toBlocking()
        .first();
    assertThat(employees).isEqualTo(new Employee("alice", "Alice Allison"));
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

  @Test public void queryInitialValueAndTriggerUsesScheduler() {
    scheduler.runTasksImmediately(false);

    db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
    o.assertNoMoreEvents();
    scheduler.triggerActions();
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();

    db.insert(TABLE_EMPLOYEE, employee("john", "John Johnson"));
    o.assertNoMoreEvents();
    scheduler.triggerActions();
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .hasRow("john", "John Johnson")
        .isExhausted();
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

  @Test public void queryNotNotifiedWhenQueryTransformerUnsubscribes() {
    db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();

    killSwitch.onNext(null);
    o.assertIsCompleted();

    db.insert(TABLE_EMPLOYEE, employee("john", "John Johnson"));
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

  @Test public void executeSqlNoTrigger() {
    db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES)
        .skip(1) // Skip initial
        .subscribe(o);

    db.execute("UPDATE " + TABLE_EMPLOYEE + " SET " + TestDb.EmployeeTable.NAME + " = 'Zach'");
    o.assertNoMoreEvents();
  }

  @Test public void executeSqlWithArgsNoTrigger() {
    db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES)
        .skip(1) // Skip initial
        .subscribe(o);

    db.execute("UPDATE " + TABLE_EMPLOYEE + " SET " + TestDb.EmployeeTable.NAME + " = ?", "Zach");
    o.assertNoMoreEvents();
  }

  @Test public void executeSqlAndTrigger() {
    db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();

    db.executeAndTrigger(TABLE_EMPLOYEE,
        "UPDATE " + TABLE_EMPLOYEE + " SET " + TestDb.EmployeeTable.NAME + " = 'Zach'");
    o.assertCursor()
        .hasRow("alice", "Zach")
        .hasRow("bob", "Zach")
        .hasRow("eve", "Zach")
        .isExhausted();
  }

  @Test public void executeSqlAndTriggerMultipleTables() {
    db.createQuery(TABLE_MANAGER, SELECT_MANAGER_LIST).subscribe(o);
    o.assertCursor()
        .hasRow("Eve Evenson", "Alice Allison")
        .isExhausted();
    final RecordingObserver employeeObserver = new RecordingObserver();
    db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(employeeObserver);
    employeeObserver.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();

    final Set<String> tablesToTrigger = Collections.unmodifiableSet(new HashSet<>(BOTH_TABLES));
    db.executeAndTrigger(tablesToTrigger,
        "UPDATE " + TABLE_EMPLOYEE + " SET " + TestDb.EmployeeTable.NAME + " = 'Zach'");

    o.assertCursor()
        .hasRow("Zach", "Zach")
        .isExhausted();
    employeeObserver.assertCursor()
        .hasRow("alice", "Zach")
        .hasRow("bob", "Zach")
        .hasRow("eve", "Zach")
        .isExhausted();
  }

  @Test public void executeSqlAndTriggerWithNoTables() {
    db.createQuery(TABLE_MANAGER, SELECT_MANAGER_LIST).subscribe(o);
    o.assertCursor()
        .hasRow("Eve Evenson", "Alice Allison")
        .isExhausted();

    db.executeAndTrigger(Collections.<String>emptySet(),
        "UPDATE " + TABLE_EMPLOYEE + " SET " + TestDb.EmployeeTable.NAME + " = 'Zach'");

    o.assertNoMoreEvents();
  }

  @Test public void executeSqlThrowsAndDoesNotTrigger() {
    db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES)
        .skip(1) // Skip initial
        .subscribe(o);

    try {
      db.executeAndTrigger(TABLE_EMPLOYEE,
          "UPDATE not_a_table SET " + TestDb.EmployeeTable.NAME + " = 'Zach'");
      fail();
    } catch (SQLException ignored) {
    }
    o.assertNoMoreEvents();
  }

  @Test public void executeSqlWithArgsAndTrigger() {
    db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();

    db.executeAndTrigger(TABLE_EMPLOYEE,
        "UPDATE " + TABLE_EMPLOYEE + " SET " + TestDb.EmployeeTable.NAME + " = ?", "Zach");
    o.assertCursor()
        .hasRow("alice", "Zach")
        .hasRow("bob", "Zach")
        .hasRow("eve", "Zach")
        .isExhausted();
  }

  @Test public void executeSqlWithArgsThrowsAndDoesNotTrigger() {
    db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES)
        .skip(1) // Skip initial
        .subscribe(o);

    try {
      db.executeAndTrigger(TABLE_EMPLOYEE,
          "UPDATE not_a_table SET " + TestDb.EmployeeTable.NAME + " = ?", "Zach");
      fail();
    } catch (SQLException ignored) {
    }
    o.assertNoMoreEvents();
  }

  @Test public void executeSqlWithArgsAndTriggerWithMultipleTables() {
    db.createQuery(TABLE_MANAGER, SELECT_MANAGER_LIST).subscribe(o);
    o.assertCursor()
        .hasRow("Eve Evenson", "Alice Allison")
        .isExhausted();
    final RecordingObserver employeeObserver = new RecordingObserver();
    db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(employeeObserver);
    employeeObserver.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();

    final Set<String> tablesToTrigger = Collections.unmodifiableSet(new HashSet<>(BOTH_TABLES));
    db.executeAndTrigger(tablesToTrigger,
        "UPDATE " + TABLE_EMPLOYEE + " SET " + TestDb.EmployeeTable.NAME + " = ?", "Zach");

    o.assertCursor()
        .hasRow("Zach", "Zach")
        .isExhausted();
    employeeObserver.assertCursor()
        .hasRow("alice", "Zach")
        .hasRow("bob", "Zach")
        .hasRow("eve", "Zach")
        .isExhausted();
  }

  @Test public void executeSqlWithArgsAndTriggerWithNoTables() {
    db.createQuery(BOTH_TABLES, SELECT_MANAGER_LIST).subscribe(o);
    o.assertCursor()
        .hasRow("Eve Evenson", "Alice Allison")
        .isExhausted();

    db.executeAndTrigger(Collections.<String>emptySet(),
        "UPDATE " + TABLE_EMPLOYEE + " SET " + TestDb.EmployeeTable.NAME + " = ?", "Zach");

    o.assertNoMoreEvents();
  }

  @Test public void executeInsertAndTrigger() {
    SQLiteStatement statement = real.compileStatement("INSERT INTO " + TABLE_EMPLOYEE + " ("
        + TestDb.EmployeeTable.NAME + ", " + TestDb.EmployeeTable.USERNAME + ") "
        + "VALUES ('Chad Chadson', 'chad')");

    db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();

    db.executeInsert(TABLE_EMPLOYEE, statement);
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .hasRow("chad", "Chad Chadson")
        .isExhausted();
  }

  @Test public void executeInsertAndDontTrigger() {
    SQLiteStatement statement = real.compileStatement("INSERT OR IGNORE INTO " + TABLE_EMPLOYEE + " ("
        + TestDb.EmployeeTable.NAME + ", " + TestDb.EmployeeTable.USERNAME + ") "
        + "VALUES ('Alice Allison', 'alice')");

    db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();

    db.executeInsert(TABLE_EMPLOYEE, statement);
    o.assertNoMoreEvents();
  }

  @Test public void executeInsertAndTriggerMultipleTables() {
    SQLiteStatement statement = real.compileStatement("INSERT INTO " + TABLE_EMPLOYEE + " ("
        + TestDb.EmployeeTable.NAME + ", " + TestDb.EmployeeTable.USERNAME + ") "
        + "VALUES ('Chad Chadson', 'chad')");

    final RecordingObserver managerObserver = new RecordingObserver();
    db.createQuery(TABLE_MANAGER, SELECT_MANAGER_LIST).subscribe(managerObserver);
    managerObserver.assertCursor()
        .hasRow("Eve Evenson", "Alice Allison")
        .isExhausted();

    db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();

    final Set<String> employeeAndManagerTables = Collections.unmodifiableSet(new HashSet<>(BOTH_TABLES));
    db.executeInsert(employeeAndManagerTables, statement);

    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .hasRow("chad", "Chad Chadson")
        .isExhausted();
    managerObserver.assertCursor()
        .hasRow("Eve Evenson", "Alice Allison")
        .isExhausted();
  }

  @Test public void executeInsertAndTriggerNoTables() {
    SQLiteStatement statement = real.compileStatement("INSERT INTO " + TABLE_EMPLOYEE + " ("
        + TestDb.EmployeeTable.NAME + ", " + TestDb.EmployeeTable.USERNAME + ") "
        + "VALUES ('Chad Chadson', 'chad')");

    db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();

    db.executeInsert(Collections.<String>emptySet(), statement);

    o.assertNoMoreEvents();
  }

  @Test public void executeInsertThrowsAndDoesNotTrigger() {
    SQLiteStatement statement = real.compileStatement("INSERT INTO " + TABLE_EMPLOYEE + " ("
        + TestDb.EmployeeTable.NAME + ", " + TestDb.EmployeeTable.USERNAME + ") "
        + "VALUES ('Alice Allison', 'alice')");

    db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES)
        .skip(1) // Skip initial
        .subscribe(o);

    try {
      db.executeInsert(TABLE_EMPLOYEE, statement);
      fail();
    } catch (SQLException ignored) {
    }
    o.assertNoMoreEvents();
  }

  @Test public void executeInsertWithArgsAndTrigger() {
    SQLiteStatement statement = real.compileStatement("INSERT INTO " + TABLE_EMPLOYEE + " ("
        + TestDb.EmployeeTable.NAME + ", " + TestDb.EmployeeTable.USERNAME + ") VALUES (?, ?)");
    statement.bindString(1, "Chad Chadson");
    statement.bindString(2, "chad");

    db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();

    db.executeInsert(TABLE_EMPLOYEE, statement);
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .hasRow("chad", "Chad Chadson")
        .isExhausted();
  }

  @Test public void executeInsertWithArgsThrowsAndDoesNotTrigger() {
    SQLiteStatement statement = real.compileStatement("INSERT INTO " + TABLE_EMPLOYEE + " ("
        + TestDb.EmployeeTable.NAME + ", " + TestDb.EmployeeTable.USERNAME + ") VALUES (?, ?)");
    statement.bindString(1, "Alice Aliison");
    statement.bindString(2, "alice");

    db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES)
        .skip(1) // Skip initial
        .subscribe(o);

    try {
      db.executeInsert(TABLE_EMPLOYEE, statement);
      fail();
    } catch (SQLException ignored) {
    }
    o.assertNoMoreEvents();
  }

  @TargetApi(Build.VERSION_CODES.HONEYCOMB)
  @SdkSuppress(minSdkVersion = Build.VERSION_CODES.HONEYCOMB)
  @Test public void executeUpdateDeleteAndTrigger() {
    SQLiteStatement statement = real.compileStatement(
        "UPDATE " + TABLE_EMPLOYEE + " SET " + TestDb.EmployeeTable.NAME + " = 'Zach'");

    db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();

    db.executeUpdateDelete(TABLE_EMPLOYEE, statement);
    o.assertCursor()
        .hasRow("alice", "Zach")
        .hasRow("bob", "Zach")
        .hasRow("eve", "Zach")
        .isExhausted();
  }

  @TargetApi(Build.VERSION_CODES.HONEYCOMB)
  @SdkSuppress(minSdkVersion = Build.VERSION_CODES.HONEYCOMB)
  @Test public void executeUpdateDeleteAndDontTrigger() {
    SQLiteStatement statement = real.compileStatement(""
        + "UPDATE " + TABLE_EMPLOYEE
        + " SET " + TestDb.EmployeeTable.NAME + " = 'Zach'"
        + " WHERE " + TestDb.EmployeeTable.NAME + " = 'Rob'");

    db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();

    db.executeUpdateDelete(TABLE_EMPLOYEE, statement);
    o.assertNoMoreEvents();
  }

  @TargetApi(Build.VERSION_CODES.HONEYCOMB)
  @SdkSuppress(minSdkVersion = Build.VERSION_CODES.HONEYCOMB)
  @Test public void executeUpdateDeleteAndTriggerWithMultipleTables() {
    SQLiteStatement statement = real.compileStatement(
        "UPDATE " + TABLE_EMPLOYEE + " SET " + TestDb.EmployeeTable.NAME + " = 'Zach'");


    final RecordingObserver managerObserver = new RecordingObserver();
    db.createQuery(TABLE_MANAGER, SELECT_MANAGER_LIST).subscribe(managerObserver);
    managerObserver.assertCursor()
        .hasRow("Eve Evenson", "Alice Allison")
        .isExhausted();

    db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();

    final Set<String> employeeAndManagerTables = Collections.unmodifiableSet(new HashSet<>(BOTH_TABLES));
    db.executeUpdateDelete(employeeAndManagerTables, statement);

    o.assertCursor()
        .hasRow("alice", "Zach")
        .hasRow("bob", "Zach")
        .hasRow("eve", "Zach")
        .isExhausted();
    managerObserver.assertCursor()
        .hasRow("Zach", "Zach")
        .isExhausted();
  }

  @TargetApi(Build.VERSION_CODES.HONEYCOMB)
  @SdkSuppress(minSdkVersion = Build.VERSION_CODES.HONEYCOMB)
  @Test public void executeUpdateDeleteAndTriggerWithNoTables() {
    SQLiteStatement statement = real.compileStatement(
        "UPDATE " + TABLE_EMPLOYEE + " SET " + TestDb.EmployeeTable.NAME + " = 'Zach'");

    db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();

    db.executeUpdateDelete(Collections.<String>emptySet(), statement);

    o.assertNoMoreEvents();
  }

  @TargetApi(Build.VERSION_CODES.HONEYCOMB)
  @SdkSuppress(minSdkVersion = Build.VERSION_CODES.HONEYCOMB)
  @Test public void executeUpdateDeleteThrowsAndDoesNotTrigger() {
    SQLiteStatement statement = real.compileStatement(
        "UPDATE " + TABLE_EMPLOYEE + " SET " + TestDb.EmployeeTable.USERNAME + " = 'alice'");

    db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES)
        .skip(1) // Skip initial
        .subscribe(o);

    try {
      db.executeUpdateDelete(TABLE_EMPLOYEE, statement);
      fail();
    } catch (SQLException ignored) {
    }
    o.assertNoMoreEvents();
  }

  @TargetApi(Build.VERSION_CODES.HONEYCOMB)
  @SdkSuppress(minSdkVersion = Build.VERSION_CODES.HONEYCOMB)
  @Test public void executeUpdateDeleteWithArgsAndTrigger() {
    SQLiteStatement statement = real.compileStatement(
        "UPDATE " + TABLE_EMPLOYEE + " SET " + TestDb.EmployeeTable.NAME + " = ?");
    statement.bindString(1, "Zach");

    db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();

    db.executeUpdateDelete(TABLE_EMPLOYEE, statement);
    o.assertCursor()
        .hasRow("alice", "Zach")
        .hasRow("bob", "Zach")
        .hasRow("eve", "Zach")
        .isExhausted();
  }

  @TargetApi(Build.VERSION_CODES.HONEYCOMB)
  @SdkSuppress(minSdkVersion = Build.VERSION_CODES.HONEYCOMB)
  @Test public void executeUpdateDeleteWithArgsThrowsAndDoesNotTrigger() {
    SQLiteStatement statement = real.compileStatement(
        "UPDATE " + TABLE_EMPLOYEE + " SET " + TestDb.EmployeeTable.USERNAME + " = ?");
    statement.bindString(1, "alice");

    db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES)
        .skip(1) // Skip initial
        .subscribe(o);

    try {
      db.executeUpdateDelete(TABLE_EMPLOYEE, statement);
      fail();
    } catch (SQLException ignored) {
    }
    o.assertNoMoreEvents();
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

  @Test public void transactionCreatedFromTransactionNotificationWorks() {
    // Tests the case where a transaction is created in the subscriber to a query which gets
    // notified as the result of another transaction being committed. With improper ordering, this
    // can result in creating a new transaction before the old is committed on the underlying DB.

    db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES)
        .subscribe(new Action1<Query>() {
          @Override public void call(Query query) {
            db.newTransaction().end();
          }
        });

    Transaction transaction = db.newTransaction();
    try {
      db.insert(TABLE_EMPLOYEE, employee("john", "John Johnson"));
      transaction.markSuccessful();
    } finally {
      transaction.end();
    }
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
    //noinspection CheckResult
    db.newTransaction();
    try {
      //noinspection CheckResult
      db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES);
      fail();
    } catch (IllegalStateException e) {
      assertThat(e.getMessage()).startsWith("Cannot create observable query in transaction.");
    }
  }

  @Test public void querySubscribedToDuringTransactionThrows() {
    Observable<Query> query = db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES);

    //noinspection CheckResult
    db.newTransaction();
    query.subscribe(o);
    o.assertErrorContains("Cannot subscribe to observable query in a transaction.");
  }

  @Test public void querySubscribedToDuringTransactionThrowsWithBackpressure() {
    o.doRequest(0);

    Observable<Query> query = db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES);

    //noinspection CheckResult
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

  @TargetApi(Build.VERSION_CODES.HONEYCOMB)
  @SdkSuppress(minSdkVersion = Build.VERSION_CODES.HONEYCOMB)
  @Test public void nonExclusiveTransactionWorks() throws InterruptedException {
    final CountDownLatch transactionStarted = new CountDownLatch(1);
    final CountDownLatch transactionProceed = new CountDownLatch(1);
    final CountDownLatch transactionCompleted = new CountDownLatch(1);

    new Thread() {
      @Override public void run() {
        Transaction transaction = db.newNonExclusiveTransaction();
        transactionStarted.countDown();
        try {
          db.insert(TABLE_EMPLOYEE, employee("hans", "Hans Hanson"));
          transactionProceed.await(10, SECONDS);
        } catch (InterruptedException e) {
          throw new RuntimeException("Exception in transaction thread", e);
        }
        transaction.markSuccessful();
        transaction.close();
        transactionCompleted.countDown();
      }
    }.start();

    assertThat(transactionStarted.await(10, SECONDS)).isTrue();

    //Simple query
    Employee employees = db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES + " LIMIT 1")
            .lift(Query.mapToOne(Employee.MAPPER))
            .toBlocking()
            .first();
    assertThat(employees).isEqualTo(new Employee("alice", "Alice Allison"));

    transactionProceed.countDown();
    assertThat(transactionCompleted.await(10, SECONDS)).isTrue();
  }

  @Test public void backpressureSupportedWhenConsumerSlow() {
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

  @Test public void backpressureSupportedWhenSchedulerSlow() {
    db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();

    // Switch the scheduler to queue actions.
    scheduler.runTasksImmediately(false);

    // Shotgun twice as many insertions as the scheduler queue can handle.
    for (int i = 0; i < RxRingBuffer.SIZE * 2; i++) {
      db.insert(TABLE_EMPLOYEE, employee("user" + i, "name" + i));
    }

    scheduler.triggerActions();

    // Assert we got all the events from the queue plus the one buffered from backpressure.
    for (int i = 0; i < RxRingBuffer.SIZE + 1; i++) {
      o.assertCursor(); // Ignore contents, just assert we got notified.
    }
  }

  @Test public void badQueryThrows() {
    try {
      //noinspection CheckResult
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
