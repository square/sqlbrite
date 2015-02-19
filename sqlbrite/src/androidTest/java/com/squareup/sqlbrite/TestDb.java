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
import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

final class TestDb extends SQLiteOpenHelper {
  static final String TABLE_EMPLOYEE = "employee";
  static final String TABLE_MANAGER = "manager";

  interface EmployeeTable {
    String ID = "_id";
    String USERNAME = "username";
    String NAME = "name";
  }

  interface ManagerTable {
    String ID = "_id";
    String EMPLOYEE_ID = "employee_id";
    String MANAGER_ID = "manager_id";
  }

  private static final String CREATE_EMPLOYEE = "CREATE TABLE " + TABLE_EMPLOYEE + " ("
      + EmployeeTable.ID + " INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, "
      + EmployeeTable.USERNAME + " TEXT NOT NULL UNIQUE, "
      + EmployeeTable.NAME + " TEXT NOT NULL)";
  private static final String CREATE_MANAGER = "CREATE TABLE " + TABLE_MANAGER + " ("
      + ManagerTable.ID + " INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, "
      + ManagerTable.EMPLOYEE_ID + " INTEGER NOT NULL UNIQUE REFERENCES " + TABLE_EMPLOYEE + "(" + EmployeeTable.ID + "), "
      + ManagerTable.MANAGER_ID + " INTEGER NOT NULL REFERENCES " + TABLE_EMPLOYEE + "(" + EmployeeTable.ID + "))";

  long aliceId;
  long bobId;
  long eveId;

  TestDb(Context context) {
    super(context, null /* memory */, null /* cursor factory */, 1 /* version */);
  }

  @Override public void onCreate(SQLiteDatabase db) {
    db.execSQL("PRAGMA foreign_keys=ON");

    db.execSQL(CREATE_EMPLOYEE);
    aliceId = db.insert(TABLE_EMPLOYEE, null, employee("alice", "Alice Allison"));
    bobId = db.insert(TABLE_EMPLOYEE, null, employee("bob", "Bob Bobberson"));
    eveId = db.insert(TABLE_EMPLOYEE, null, employee("eve", "Eve Evenson"));

    db.execSQL(CREATE_MANAGER);
    db.insert(TABLE_MANAGER, null, manager(eveId, aliceId));
  }

  static ContentValues employee(String username, String name) {
    ContentValues values = new ContentValues();
    values.put(EmployeeTable.USERNAME, username);
    values.put(EmployeeTable.NAME, name);
    return values;
  }

  static ContentValues manager(long employeeId, long managerId) {
    ContentValues values = new ContentValues();
    values.put(ManagerTable.EMPLOYEE_ID, employeeId);
    values.put(ManagerTable.MANAGER_ID, managerId);
    return values;
  }

  @Override public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
  }
}
