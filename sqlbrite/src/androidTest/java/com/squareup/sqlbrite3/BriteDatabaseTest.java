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
import android.arch.persistence.db.SimpleSQLiteQuery;
import android.arch.persistence.db.SupportSQLiteDatabase;
import android.arch.persistence.db.SupportSQLiteOpenHelper;
import android.arch.persistence.db.SupportSQLiteOpenHelper.Configuration;
import android.arch.persistence.db.SupportSQLiteOpenHelper.Factory;
import android.arch.persistence.db.SupportSQLiteStatement;
import android.arch.persistence.db.framework.FrameworkSQLiteOpenHelperFactory;
import android.content.ContentValues;
import android.database.Cursor;
import android.database.SQLException;
import android.database.sqlite.SQLiteException;
import android.os.Build;
import android.support.test.InstrumentationRegistry;
import android.support.test.filters.SdkSuppress;
import android.support.test.runner.AndroidJUnit4;
import com.squareup.sqlbrite3.BriteDatabase.Transaction;
import com.squareup.sqlbrite3.RecordingObserver.CursorAssert;
import com.squareup.sqlbrite3.TestDb.Employee;
import io.reactivex.Observable;
import io.reactivex.ObservableSource;
import io.reactivex.ObservableTransformer;
import io.reactivex.functions.Consumer;
import io.reactivex.subjects.PublishSubject;
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

import static android.database.sqlite.SQLiteDatabase.CONFLICT_IGNORE;
import static com.google.common.truth.Truth.assertThat;
import static com.squareup.sqlbrite3.SqlBrite.Query;
import static com.squareup.sqlbrite3.TestDb.BOTH_TABLES;
import static com.squareup.sqlbrite3.TestDb.EmployeeTable.NAME;
import static com.squareup.sqlbrite3.TestDb.EmployeeTable.USERNAME;
import static com.squareup.sqlbrite3.TestDb.SELECT_EMPLOYEES;
import static com.squareup.sqlbrite3.TestDb.SELECT_MANAGER_LIST;
import static com.squareup.sqlbrite3.TestDb.TABLE_EMPLOYEE;
import static com.squareup.sqlbrite3.TestDb.TABLE_MANAGER;
import static com.squareup.sqlbrite3.TestDb.employee;
import static com.squareup.sqlbrite3.TestDb.manager;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.fail;

@RunWith(AndroidJUnit4.class) //
public final class BriteDatabaseTest {
  private final TestDb testDb = new TestDb();
  private final List<String> logs = new ArrayList<>();
  private final RecordingObserver o = new RecordingObserver();
  private final TestScheduler scheduler = new TestScheduler();
  private final PublishSubject<Object> killSwitch = PublishSubject.create();

  @Rule public final TemporaryFolder dbFolder = new TemporaryFolder();

  private SupportSQLiteDatabase real;
  private BriteDatabase db;

  @Before public void setUp() throws IOException {
    Configuration configuration = Configuration.builder(InstrumentationRegistry.getContext())
        .callback(testDb)
        .version(1)
        .name(dbFolder.newFile().getPath())
        .build();

    Factory factory = new FrameworkSQLiteOpenHelperFactory();
    SupportSQLiteOpenHelper helper = factory.create(configuration);
    real = helper.getWritableDatabase();

    SqlBrite.Logger logger = new SqlBrite.Logger() {
      @Override public void log(String message) {
        logs.add(message);
      }
    };
    ObservableTransformer<Query, Query> queryTransformer =
        new ObservableTransformer<Query, Query>() {
          @Override public ObservableSource<Query> apply(Observable<Query> upstream) {
            return upstream.takeUntil(killSwitch);
          }
        };
    PublishSubject<Set<String>> triggers = PublishSubject.create();
    db = new BriteDatabase(helper, logger, triggers, triggers, scheduler, queryTransformer);
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

  @Test public void queryWithQueryObject() {
    db.createQuery(TABLE_EMPLOYEE, new SimpleSQLiteQuery(SELECT_EMPLOYEES)).subscribe(o);
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();
  }

  @Test public void queryMapToList() {
    List<Employee> employees = db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES)
        .mapToList(Employee.MAPPER)
        .blockingFirst();
    assertThat(employees).containsExactly( //
        new Employee("alice", "Alice Allison"), //
        new Employee("bob", "Bob Bobberson"), //
        new Employee("eve", "Eve Evenson"));
  }

  @Test public void queryMapToOne() {
    Employee employees = db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES + " LIMIT 1")
        .mapToOne(Employee.MAPPER)
        .blockingFirst();
    assertThat(employees).isEqualTo(new Employee("alice", "Alice Allison"));
  }

  @Test public void queryMapToOneOrDefault() {
    Employee employees = db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES + " LIMIT 1")
        .mapToOneOrDefault(Employee.MAPPER, new Employee("wrong", "Wrong Person"))
        .blockingFirst();
    assertThat(employees).isEqualTo(new Employee("alice", "Alice Allison"));
  }

  @Test public void badQueryCallsError() {
    // safeSubscribe is needed because the error occurs in onNext and will otherwise bubble up
    // to the thread exception handler.
    db.createQuery(TABLE_EMPLOYEE, "SELECT * FROM missing").safeSubscribe(o);
    o.assertErrorContains("no such table: missing");
  }

  @Test public void queryWithArgs() {
    db.createQuery(
        TABLE_EMPLOYEE, SELECT_EMPLOYEES + " WHERE " + USERNAME + " = ?", "bob")
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

    killSwitch.onNext("kill");
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

  @Test public void queryMultipleTablesWithQueryObject() {
    db.createQuery(BOTH_TABLES, new SimpleSQLiteQuery(SELECT_MANAGER_LIST)).subscribe(o);
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
    db.insert(TABLE_MANAGER, manager(testDb.bobId, testDb.eveId));
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

  @Test public void queryNotNotifiedAfterDispose() {
    db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();
    o.dispose();

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

    db.execute("UPDATE " + TABLE_EMPLOYEE + " SET " + NAME + " = 'Zach'");
    o.assertNoMoreEvents();
  }

  @Test public void executeSqlWithArgsNoTrigger() {
    db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES)
        .skip(1) // Skip initial
        .subscribe(o);

    db.execute("UPDATE " + TABLE_EMPLOYEE + " SET " + NAME + " = ?", "Zach");
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
        "UPDATE " + TABLE_EMPLOYEE + " SET " + NAME + " = 'Zach'");
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
        "UPDATE " + TABLE_EMPLOYEE + " SET " + NAME + " = 'Zach'");

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
        "UPDATE " + TABLE_EMPLOYEE + " SET " + NAME + " = 'Zach'");

    o.assertNoMoreEvents();
  }

  @Test public void executeSqlThrowsAndDoesNotTrigger() {
    db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES)
        .skip(1) // Skip initial
        .subscribe(o);

    try {
      db.executeAndTrigger(TABLE_EMPLOYEE,
          "UPDATE not_a_table SET " + NAME + " = 'Zach'");
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
        "UPDATE " + TABLE_EMPLOYEE + " SET " + NAME + " = ?", "Zach");
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
          "UPDATE not_a_table SET " + NAME + " = ?", "Zach");
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
        "UPDATE " + TABLE_EMPLOYEE + " SET " + NAME + " = ?", "Zach");

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
        "UPDATE " + TABLE_EMPLOYEE + " SET " + NAME + " = ?", "Zach");

    o.assertNoMoreEvents();
  }

  @Test public void executeInsertAndTrigger() {
    SupportSQLiteStatement statement = real.compileStatement("INSERT INTO "
        + TABLE_EMPLOYEE + " (" + NAME + ", " + USERNAME + ") "
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
    SupportSQLiteStatement statement = real.compileStatement("INSERT OR IGNORE INTO "
        + TABLE_EMPLOYEE + " (" + NAME + ", " + USERNAME + ") "
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
    SupportSQLiteStatement statement = real.compileStatement("INSERT INTO "
        + TABLE_EMPLOYEE + " (" + NAME + ", " + USERNAME + ") "
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

    final Set<String> employeeAndManagerTables = Collections.unmodifiableSet(new HashSet<>(
        BOTH_TABLES));
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
    SupportSQLiteStatement statement = real.compileStatement("INSERT INTO "
        + TABLE_EMPLOYEE + " (" + NAME + ", " + USERNAME + ") "
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
    SupportSQLiteStatement statement = real.compileStatement("INSERT INTO "
        + TABLE_EMPLOYEE + " (" + NAME + ", " + USERNAME + ") "
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
    SupportSQLiteStatement statement = real.compileStatement("INSERT INTO "
        + TABLE_EMPLOYEE + " (" + NAME + ", " + USERNAME + ") VALUES (?, ?)");
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
    SupportSQLiteStatement statement = real.compileStatement("INSERT INTO "
        + TABLE_EMPLOYEE + " (" + NAME + ", " + USERNAME + ") VALUES (?, ?)");
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
    SupportSQLiteStatement statement = real.compileStatement(
        "UPDATE " + TABLE_EMPLOYEE + " SET " + NAME + " = 'Zach'");

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
    SupportSQLiteStatement statement = real.compileStatement(""
        + "UPDATE " + TABLE_EMPLOYEE
        + " SET " + NAME + " = 'Zach'"
        + " WHERE " + NAME + " = 'Rob'");

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
    SupportSQLiteStatement statement = real.compileStatement(
        "UPDATE " + TABLE_EMPLOYEE + " SET " + NAME + " = 'Zach'");


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
    SupportSQLiteStatement statement = real.compileStatement(
        "UPDATE " + TABLE_EMPLOYEE + " SET " + NAME + " = 'Zach'");

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
    SupportSQLiteStatement statement = real.compileStatement(
        "UPDATE " + TABLE_EMPLOYEE + " SET " + USERNAME + " = 'alice'");

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
    SupportSQLiteStatement statement = real.compileStatement(
        "UPDATE " + TABLE_EMPLOYEE + " SET " + NAME + " = ?");
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
    SupportSQLiteStatement statement = real.compileStatement(
        "UPDATE " + TABLE_EMPLOYEE + " SET " + USERNAME + " = ?");
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
        .subscribe(new Consumer<Query>() {
          @Override public void accept(Query query) {
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
        db.insert(TABLE_MANAGER, manager(testDb.aliceId, testDb.bobId));
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
            .blockingFirst();
    assertThat(employees).isEqualTo(new Employee("alice", "Alice Allison"));

    transactionProceed.countDown();
    assertThat(transactionCompleted.await(10, SECONDS)).isTrue();
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
