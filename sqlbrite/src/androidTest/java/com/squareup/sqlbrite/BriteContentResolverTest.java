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

import android.content.ContentResolver;
import android.content.ContentValues;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.test.ProviderTestCase2;
import android.test.mock.MockContentProvider;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import rx.Subscription;
import rx.internal.util.RxRingBuffer;
import rx.subscriptions.Subscriptions;

import static com.google.common.truth.Truth.assertThat;

public final class BriteContentResolverTest
    extends ProviderTestCase2<BriteContentResolverTest.TestContentProvider> {
  private static final Uri AUTHORITY = Uri.parse("content://test_authority");
  private static final Uri TABLE = AUTHORITY.buildUpon().appendPath("test_table").build();
  private static final String KEY = "test_key";
  private static final String VALUE = "test_value";

  private final List<String> logs = new ArrayList<>();
  private final RecordingObserver o = new BlockingRecordingObserver();
  private final TestScheduler scheduler = new TestScheduler();

  private ContentResolver contentResolver;
  private BriteContentResolver db;
  private Subscription subscription;

  public BriteContentResolverTest() {
    super(TestContentProvider.class, AUTHORITY.getAuthority());
  }

  @Override protected void setUp() throws Exception {
    super.setUp();
    contentResolver = getMockContentResolver();
    subscription = Subscriptions.empty();

    SqlBrite.Logger logger = new SqlBrite.Logger() {
      @Override public void log(String message) {
        logs.add(message);
      }
    };
    db = new BriteContentResolver(contentResolver, logger, scheduler);

    getProvider().init(getContext().getContentResolver());
  }

  @Override public void tearDown() {
    o.assertNoMoreEvents();
    subscription.unsubscribe();
  }

  public void testLoggerEnabled() {
    db.setLoggingEnabled(true);

    subscription = db.createQuery(TABLE, null, null, null, null, false).subscribe(o);
    o.assertCursor().isExhausted();

    contentResolver.insert(TABLE, values("key1", "value1"));
    o.assertCursor().hasRow("key1", "value1").isExhausted();
    assertThat(logs).isNotEmpty();
  }

  public void testLoggerDisabled() {
    db.setLoggingEnabled(false);

    contentResolver.insert(TABLE, values("key1", "value1"));
    assertThat(logs).isEmpty();
  }

  public void testCreateQueryObservesInsert() {
    subscription = db.createQuery(TABLE, null, null, null, null, false).subscribe(o);
    o.assertCursor().isExhausted();

    contentResolver.insert(TABLE, values("key1", "val1"));
    o.assertCursor().hasRow("key1", "val1").isExhausted();
  }

  public void testCreateQueryObservesUpdate() {
    contentResolver.insert(TABLE, values("key1", "val1"));
    subscription = db.createQuery(TABLE, null, null, null, null, false).subscribe(o);
    o.assertCursor().hasRow("key1", "val1").isExhausted();

    contentResolver.update(TABLE, values("key1", "val2"), null, null);
    o.assertCursor().hasRow("key1", "val2").isExhausted();
  }

  public void testCreateQueryObservesDelete() {
    contentResolver.insert(TABLE, values("key1", "val1"));
    subscription = db.createQuery(TABLE, null, null, null, null, false).subscribe(o);
    o.assertCursor().hasRow("key1", "val1").isExhausted();

    contentResolver.delete(TABLE, null, null);
    o.assertCursor().isExhausted();
  }

  public void testUnsubscribeDoesNotTrigger() {
    subscription = db.createQuery(TABLE, null, null, null, null, false).subscribe(o);
    o.assertCursor().isExhausted();
    subscription.unsubscribe();

    contentResolver.insert(TABLE, values("key1", "val1"));
    o.assertNoMoreEvents();
    assertThat(logs).isEmpty();
  }

  public void testBackpressureSupportedWhenConsumerSlow() {
    contentResolver.insert(TABLE, values("key1", "val1"));
    o.doRequest(2);

    subscription = db.createQuery(TABLE, null, null, null, null, false).subscribe(o);
    o.assertCursor()
        .hasRow("key1", "val1")
        .isExhausted();

    contentResolver.insert(TABLE, values("key2", "val2"));
    o.assertCursor()
        .hasRow("key1", "val1")
        .hasRow("key2", "val2")
        .isExhausted();

    contentResolver.insert(TABLE, values("key3", "val3"));
    o.assertNoMoreEvents();

    contentResolver.insert(TABLE, values("key4", "val4"));
    o.assertNoMoreEvents();

    o.doRequest(1);
    o.assertCursor()
        .hasRow("key1", "val1")
        .hasRow("key2", "val2")
        .hasRow("key3", "val3")
        .hasRow("key4", "val4")
        .isExhausted();

    contentResolver.insert(TABLE, values("key5", "val5"));
    o.assertNoMoreEvents();
    contentResolver.insert(TABLE, values("key6", "val6"));
    o.assertNoMoreEvents();

    o.doRequest(Long.MAX_VALUE);
    o.assertCursor()
        .hasRow("key1", "val1")
        .hasRow("key2", "val2")
        .hasRow("key3", "val3")
        .hasRow("key4", "val4")
        .hasRow("key5", "val5")
        .hasRow("key6", "val6")
        .isExhausted();
    o.assertNoMoreEvents();
  }

  public void testBackpressureSupportedWhenSchedulerSlow() {
    subscription = db.createQuery(TABLE, null, null, null, null, false).subscribe(o);
    o.assertCursor().isExhausted();

    // Switch the scheduler to queue actions.
    scheduler.runTasksImmediately(false);

    // Shotgun twice as many insertions as the scheduler queue can handle.
    for (int i = 0; i < RxRingBuffer.SIZE * 2; i++) {
      contentResolver.insert(TABLE, values("key" + i, "val" + i));
    }

    scheduler.triggerActions();

    // Assert we got all the events from the queue plus the one buffered from backpressure.
    for (int i = 0; i < RxRingBuffer.SIZE + 1; i++) {
      o.assertCursor(); // Ignore contents, just assert we got notified.
    }
  }

  public void testInitialValueAndTriggerUsesScheduler() {
    scheduler.runTasksImmediately(false);

    subscription = db.createQuery(TABLE, null, null, null, null, false).subscribe(o);
    o.assertNoMoreEvents();
    scheduler.triggerActions();
    o.assertCursor().isExhausted();

    contentResolver.insert(TABLE, values("key1", "val1"));
    o.assertNoMoreEvents();
    scheduler.triggerActions();
    o.assertCursor().hasRow("key1", "val1").isExhausted();
  }

  private ContentValues values(String key, String value) {
    ContentValues result = new ContentValues();
    result.put(KEY, key);
    result.put(VALUE, value);
    return result;
  }

  public static final class TestContentProvider extends MockContentProvider {
    private final Map<String, String> storage = new LinkedHashMap<>();

    private ContentResolver contentResolver;

    void init(ContentResolver contentResolver) {
      this.contentResolver = contentResolver;
    }

    @Override public Uri insert(Uri uri, ContentValues values) {
      storage.put(values.getAsString(KEY), values.getAsString(VALUE));
      contentResolver.notifyChange(uri, null);
      return Uri.parse(AUTHORITY + "/" + values.getAsString(KEY));
    }

    @Override public int update(Uri uri, ContentValues values, String selection,
        String[] selectionArgs) {
      for (String key : storage.keySet()) {
        storage.put(key, values.getAsString(VALUE));
      }
      contentResolver.notifyChange(uri, null);
      return storage.size();
    }

    @Override public int delete(Uri uri, String selection, String[] selectionArgs) {
      int result = storage.size();
      storage.clear();
      contentResolver.notifyChange(uri, null);
      return result;
    }

    @Override public Cursor query(Uri uri, String[] projection, String selection,
        String[] selectionArgs, String sortOrder) {
      MatrixCursor result = new MatrixCursor(new String[] { KEY, VALUE });
      for (Map.Entry<String, String> entry : storage.entrySet()) {
        result.addRow(new Object[] { entry.getKey(), entry.getValue() });
      }
      return result;
    }
  }
}
