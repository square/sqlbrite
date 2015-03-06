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
import android.database.ContentObserver;
import android.database.Cursor;
import android.net.Uri;
import android.os.Handler;
import android.os.Looper;
import android.support.annotation.NonNull;
import android.support.annotation.Nullable;
import android.util.Log;
import java.util.Arrays;
import rx.Observable;
import rx.Subscriber;
import rx.functions.Action0;
import rx.subscriptions.Subscriptions;

import static com.squareup.sqlbrite.SqlBrite.Logger;
import static com.squareup.sqlbrite.SqlBrite.Query;

/**
 * A lightweight wrapper around {@link ContentResolver} which allows for continuously observing
 * the result of a query.
 */
public final class SqlBriteContentProvider {
  public static SqlBriteContentProvider create(@NonNull ContentResolver contentResolver) {
    return new SqlBriteContentProvider(contentResolver);
  }

  private final Handler contentObserverHandler = new Handler(Looper.getMainLooper());
  private final ContentResolver contentResolver;

  // Not volatile because we don't care if threads don't immediately see changes to this value.
  private boolean logging;
  private volatile Logger logger;

  private SqlBriteContentProvider(ContentResolver contentResolver) {
    this.contentResolver = contentResolver;
  }

  /**
   * Control whether debug logging is enabled.
   * <p>
   * By default this method will log verbose message to {@linkplain Log Android's log}. Use a
   * custom logger by calling {@link #setLogger}.
   */
  public void setLoggingEnabled(boolean enabled) {
    if (enabled && logger == null) {
      logger = new Logger() {
        @Override public void log(String message) {
          Log.v("SqlBrite", message);
        }
      };
    }
    logging = enabled;
  }

  /**
   * Specify a custom logger for debug messages when {@linkplain #setLoggingEnabled(boolean)
   * logging is enabled}.
   */
  public void setLogger(Logger logger) {
    if (logger == null) throw new NullPointerException("logger == null");
    this.logger = logger;
  }

  /**
   * Create an observable which will notify subscribers with a {@linkplain Query query} for
   * execution. Subscribers are responsible for <b>always</b> closing {@link Cursor} instance
   * returned from the {@link Query}.
   * <p>
   * Subscribers will receive an immediate notification for initial data as well as subsequent
   * notifications for when the supplied {@code uri}'s data changes. Unsubscribe when you no longer
   * want updates to a query.
   * <p>
   * <b>Warning:</b> this method does not perform the query! Only by subscribing to the returned
   * {@link Observable} will the operation occur.
   *
   * @see ContentResolver#query(Uri, String[], String, String[], String)
   * @see ContentResolver#registerContentObserver(Uri, boolean, ContentObserver)
   */
  public Observable<Query> createQuery(@NonNull final Uri uri, @Nullable final String[] projection,
      @Nullable final String selection, @Nullable final String[] selectionArgs, @Nullable
      final String sortOrder, final boolean notifyForDescendents) {
    final Query query = new Query() {
      @Override public Cursor run() {
        return contentResolver.query(uri, projection, selection, selectionArgs, sortOrder);
      }
    };
    return Observable.create(new Observable.OnSubscribe<Query>() {
      @Override public void call(final Subscriber<? super Query> subscriber) {
        final ContentObserver observer = new ContentObserver(contentObserverHandler) {
          @Override public void onChange(boolean selfChange) {
            if (logging) {
              log("QUERY\n  uri: %s\n  projection: %s\n  selection: %s\n  selectionArgs: %s\n  "
                      + "sortOrder: %s\n  notifyForDescendents: %s", uri,
                  Arrays.toString(projection), selection, Arrays.toString(selectionArgs), sortOrder,
                  notifyForDescendents);
            }
            subscriber.onNext(query);
          }
        };
        contentResolver.registerContentObserver(uri, notifyForDescendents, observer);
        subscriber.add(Subscriptions.create(new Action0() {
          @Override public void call() {
            contentResolver.unregisterContentObserver(observer);
          }
        }));
      }
    }).startWith(query);
  }

  private void log(String message, Object... args) {
    if (args.length > 0) message = String.format(message, args);
    logger.log(message);
  }
}
