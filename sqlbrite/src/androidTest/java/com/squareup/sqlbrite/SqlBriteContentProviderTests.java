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
import rx.subscriptions.Subscriptions;

import static com.google.common.truth.Truth.assertThat;

public final class SqlBriteContentProviderTests
    extends ProviderTestCase2<SqlBriteContentProviderTests.TestContentProvider> {
  private static final Uri AUTHORITY = Uri.parse("content://test_authority");
  private static final Uri TABLE = AUTHORITY.buildUpon().appendPath("test_table").build();
  private static final String KEY = "test_key";
  private static final String VALUE = "test_value";

  private final RecordingObserver o = new RecordingObserver();

  private ContentResolver contentResolver;
  private SqlBriteContentProvider db;
  private Subscription subscription;

  public SqlBriteContentProviderTests() {
    super(TestContentProvider.class, AUTHORITY.getAuthority());
  }

  @Override protected void setUp() throws Exception {
    super.setUp();
    contentResolver = getMockContentResolver();
    db = SqlBriteContentProvider.create(contentResolver);
    getProvider().init(getContext().getContentResolver());
    db.setLoggingEnabled(true);
    subscription = Subscriptions.empty();
  }

  @Override public void tearDown() {
    o.assertNoMoreEvents();
    subscription.unsubscribe();
  }

  public void testLoggerInvalidValues() {
    try {
      db.setLogger(null);
      fail();
    } catch (NullPointerException e) {
      assertThat(e).hasMessage("logger == null");
    }
  }

  public void testLoggerEnabled() {
    final List<String> logs = new ArrayList<>();
    db.setLogger(new SqlBrite.Logger() {
      @Override public void log(String message) {
        logs.add(message);
      }
    });

    subscription = db.createQuery(TABLE, null, null, null, null, false).subscribe(o);
    o.assertCursor().isExhausted();

    contentResolver.insert(TABLE, values("key1", "value1"));
    o.assertCursor().hasRow("key1", "value1").isExhausted();
    assertThat(logs).isNotEmpty();
  }

  public void testLoggerDisabled() {
    final List<String> logs = new ArrayList<>();
    db.setLoggingEnabled(false);
    db.setLogger(new SqlBrite.Logger() {
      @Override public void log(String message) {
        logs.add(message);
      }
    });

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
    final List<String> logs = new ArrayList<>();
    db.setLogger(new SqlBrite.Logger() {
      @Override public void log(String message) {
        logs.add(message);
      }
    });

    subscription = db.createQuery(TABLE, null, null, null, null, false).subscribe(o);
    o.assertCursor().isExhausted();
    subscription.unsubscribe();

    contentResolver.insert(TABLE, values("key1", "val1"));
    o.assertNoMoreEvents();
    assertThat(logs).isEmpty();
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
