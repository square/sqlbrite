/*
 * Copyright (C) 2017 Square, Inc.
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

import android.database.sqlite.SQLiteOpenHelper;
import android.support.annotation.RestrictTo;
import java.util.Set;
import rx.Observable;
import rx.Observer;
import rx.Scheduler;

import static android.support.annotation.RestrictTo.Scope.LIBRARY;

@RestrictTo(LIBRARY)
public final class BriteDatabaseV1Factory {
  private BriteDatabaseV1Factory() {
    throw new AssertionError("No instances");
  }

  public static BriteDatabase create(SqlBrite sqlBrite, SQLiteOpenHelper helper,
      Observable<Set<String>> triggerSource, Observer<Set<String>> triggerSink,
      Scheduler scheduler) {
    return new BriteDatabase(helper, sqlBrite.logger, triggerSource, triggerSink, scheduler,
        sqlBrite.queryTransformer);
  }
}
