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
package com.example.sqlbrite.todo.db;

import android.content.ContentValues;
import android.database.Cursor;
import auto.parcel.AutoParcel;
import java.util.ArrayList;
import java.util.List;
import rx.functions.Func1;

import static com.squareup.sqlbrite.SqlBrite.Query;

@AutoParcel
public abstract class TodoItem {
  public static final String TABLE = "todo_item";

  public static final String ID = "_id";
  public static final String LIST_ID = "todo_list_id";
  public static final String DESCRIPTION = "description";
  public static final String COMPLETE = "complete";

  public abstract long id();
  public abstract long listId();
  public abstract String description();
  public abstract boolean complete();

  public static final Func1<Query, List<TodoItem>> MAP = new Func1<Query, List<TodoItem>>() {
    @Override public List<TodoItem> call(Query query) {
      Cursor cursor = query.run();
      try {
        List<TodoItem> values = new ArrayList<>(cursor.getCount());
        while (cursor.moveToNext()) {
          long id = Db.getLong(cursor, ID);
          long listId = Db.getLong(cursor, LIST_ID);
          String description = Db.getString(cursor, DESCRIPTION);
          boolean complete = Db.getBoolean(cursor, COMPLETE);
          values.add(new AutoParcel_TodoItem(id, listId, description, complete));
        }
        return values;
      } finally {
        cursor.close();
      }
    }
  };

  public static final class Builder {
    private final ContentValues values = new ContentValues();

    public Builder id(long id) {
      values.put(ID, id);
      return this;
    }

    public Builder listId(long listId) {
      values.put(LIST_ID, listId);
      return this;
    }

    public Builder description(String description) {
      values.put(DESCRIPTION, description);
      return this;
    }

    public Builder complete(boolean complete) {
      values.put(COMPLETE, complete);
      return this;
    }

    public ContentValues build() {
      return values; // TODO defensive copy?
    }
  }
}
