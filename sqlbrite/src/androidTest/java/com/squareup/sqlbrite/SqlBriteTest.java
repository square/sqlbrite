package com.squareup.sqlbrite;

import android.content.ContentValues;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.support.test.InstrumentationRegistry;
import android.support.test.runner.AndroidJUnit4;
import com.google.common.collect.Range;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import rx.Observable;
import rx.Subscription;

import static android.database.sqlite.SQLiteDatabase.CONFLICT_IGNORE;
import static com.google.common.truth.Truth.assertThat;
import static com.squareup.sqlbrite.RecordingObserver.CursorAssert;
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
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.fail;

@RunWith(AndroidJUnit4.class)
public final class SqlBriteTest {
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

  private final RecordingObserver o = new RecordingObserver();

  private TestDb helper;
  private SQLiteDatabase real;
  private SqlBrite db;

  @Before public void setUp() {
    helper = new TestDb(InstrumentationRegistry.getContext());
    real = helper.getWritableDatabase();
    db = SqlBrite.builder(helper)
        .debugLogging(true)
        .build();
  }

  @After public void tearDown() {
    o.assertNoMoreEvents();
  }

  @Test public void closePropagates() throws IOException {
    db.close();
    assertThat(real.isOpen()).isFalse();
  }

  @Test public void invalidThrottleValues() {
    SqlBrite.Builder builder = SqlBrite.builder(helper);
    try {
      builder.throttle(-1, SECONDS);
      fail();
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage()).isEqualTo("amount < 0");
    }
    try {
      builder.throttle(1, null);
      fail();
    } catch (NullPointerException e) {
      assertThat(e.getMessage()).isEqualTo("unit == null");
    }
  }

  @Test public void query() {
    db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();
  }

  @Test public void badQueryCallsError() {
    db.createQuery(TABLE_EMPLOYEE, "SELECT * FROM missing").subscribe(o);
    o.assertError("no such table: missing (code 1): , while compiling: SELECT * FROM missing");
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

  @Test public void queryObservesInsertThrottled() {
    db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
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
    assertThat(TimeUnit.NANOSECONDS.toMillis(tookNs)).isIn(Range.closed(500L, 525L));
  }

  @Test public void queryObservesInsertCustomThrottle() {
    db = SqlBrite.builder(helper)
        .throttle(200, MILLISECONDS)
        .build();

    db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
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
    assertThat(TimeUnit.NANOSECONDS.toMillis(tookNs)).isIn(Range.closed(200L, 225L));
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
    Observable<Cursor> query = db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES);
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

    db.beginTransaction();
    try {
      db.insert(TABLE_EMPLOYEE, employee("john", "John Johnson"));
      db.insert(TABLE_EMPLOYEE, employee("nick", "Nick Nickers"));
      o.assertNoMoreEvents();

      db.setTransactionSuccessful();
    } finally {
      db.endTransaction();
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
    db.beginTransaction();
    try {
      db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES);
      fail();
    } catch (IllegalStateException e) {
      assertThat(e.getMessage()).startsWith("Cannot create observable query in transaction.");
    }
  }

  @Test public void querySubscribedToDuringTransactionThrows() {
    Observable<Cursor> query = db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES);

    db.beginTransaction();
    query.subscribe(o);
    o.assertError("Cannot subscribe to observable query in a transaction.");
  }

  @Test public void endTransactionWithNoBeginFails() {
    try {
      db.endTransaction();
      fail();
    } catch (IllegalStateException e) {
      assertThat(e.getMessage()).isEqualTo("Not in transaction.");
    }
  }

  @Test public void querySubscribedToDuringTransactionOnDifferentThread()
      throws InterruptedException {
    db.beginTransaction();

    final CountDownLatch latch = new CountDownLatch(1);
    new Thread() {
      @Override public void run() {
        db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
        latch.countDown();
      }
    }.start();

    Thread.sleep(500); // Wait for the thread to block on initial query.
    o.assertNoMoreEvents();

    db.endTransaction(); // Allow other queries to continue.
    latch.await(500, MILLISECONDS); // Wait for thread to observe initial query.

    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();
  }

  @Test public void queryCreatedBeforeTransactionButSubscribedAfter() {
    Observable<Cursor> query = db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES);

    db.beginTransaction();
    try {
      db.insert(TABLE_EMPLOYEE, employee("john", "John Johnson"));
      db.insert(TABLE_EMPLOYEE, employee("nick", "Nick Nickers"));
      db.setTransactionSuccessful();
    } finally {
      db.endTransaction();
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
    db.beginTransaction();
    try {
      db.setTransactionSuccessful();
      assertCursor(db.query(SELECT_EMPLOYEES))
          .hasRow("alice", "Alice Allison")
          .hasRow("bob", "Bob Bobberson")
          .hasRow("eve", "Eve Evenson")
          .isExhausted();
    } finally {
      db.endTransaction();
    }
  }

  @Test public void synchronousQueryDuringTransactionSeesChanges() {
    db.beginTransaction();
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

      db.setTransactionSuccessful();
    } finally {
      db.endTransaction();
    }
  }

  @Test public void nestedTransactionsOnlyNotifyOnce() {
    db.createQuery(TABLE_EMPLOYEE, SELECT_EMPLOYEES).subscribe(o);
    o.assertCursor()
        .hasRow("alice", "Alice Allison")
        .hasRow("bob", "Bob Bobberson")
        .hasRow("eve", "Eve Evenson")
        .isExhausted();

    db.beginTransaction();
    try {
      db.insert(TABLE_EMPLOYEE, employee("john", "John Johnson"));

      db.beginTransaction();
      try {
        db.insert(TABLE_EMPLOYEE, employee("nick", "Nick Nickers"));
        db.setTransactionSuccessful();
      } finally {
        db.endTransaction();
      }

      db.setTransactionSuccessful();
    } finally {
      db.endTransaction();
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

    db.beginTransaction();
    try {

      db.beginTransaction();
      try {
        db.insert(TABLE_EMPLOYEE, employee("john", "John Johnson"));
        db.setTransactionSuccessful();
      } finally {
        db.endTransaction();
      }

      db.beginTransaction();
      try {
        db.insert(TABLE_MANAGER, manager(helper.aliceId, helper.bobId));
        db.setTransactionSuccessful();
      } finally {
        db.endTransaction();
      }

      db.setTransactionSuccessful();
    } finally {
      db.endTransaction();
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

    db.beginTransaction();
    try {
      db.setTransactionSuccessful();
    } finally {
      db.endTransaction();
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

    db.beginTransaction();
    try {
      db.insert(TABLE_EMPLOYEE, employee("john", "John Johnson"));
      db.insert(TABLE_EMPLOYEE, employee("nick", "Nick Nickers"));
      // No call to set successful.
    } finally {
      db.endTransaction();
    }
    o.assertNoMoreEvents();
  }

  private static CursorAssert assertCursor(Cursor cursor) {
    return new CursorAssert(cursor);
  }
}
