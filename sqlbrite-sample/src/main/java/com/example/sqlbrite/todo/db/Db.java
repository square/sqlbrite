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

import android.database.Cursor;

public final class Db {
  public static final int BOOLEAN_FALSE = 0;
  public static final int BOOLEAN_TRUE = 1;

  public static String getString(Cursor cursor, String columnName) {
    return cursor.getString(cursor.getColumnIndexOrThrow(columnName));
  }

  public static boolean getBoolean(Cursor cursor, String columnName) {
    return getInt(cursor, columnName) == BOOLEAN_TRUE;
  }

  public static long getLong(Cursor cursor, String columnName) {
    return cursor.getLong(cursor.getColumnIndexOrThrow(columnName));
  }

  public static int getInt(Cursor cursor, String columnName) {
    return cursor.getInt(cursor.getColumnIndexOrThrow(columnName));
  }

  private Db() {
    throw new AssertionError("No instances.");
  }
}
