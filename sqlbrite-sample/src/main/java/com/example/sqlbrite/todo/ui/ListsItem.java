package com.example.sqlbrite.todo.ui;

import android.database.Cursor;
import auto.parcel.AutoParcel;
import com.example.sqlbrite.todo.db.Db;
import com.example.sqlbrite.todo.db.TodoItem;
import com.example.sqlbrite.todo.db.TodoList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import rx.functions.Func1;

@AutoParcel
abstract class ListsItem {
  private static String ALIAS_LIST = "list";
  private static String ALIAS_ITEM = "item";

  private static String LIST_ID = ALIAS_LIST + "." + TodoList.ID;
  private static String LIST_NAME = ALIAS_LIST + "." + TodoList.NAME;
  private static String ITEM_COUNT = "item_count";
  private static String ITEM_ID = ALIAS_ITEM + "." + TodoItem.ID;
  private static String ITEM_LIST_ID = ALIAS_ITEM + "." + TodoItem.LIST_ID;

  public static Collection<String> TABLES = Arrays.asList(TodoList.TABLE, TodoItem.TABLE);
  public static String QUERY = ""
      + "SELECT " + LIST_ID + ", " + LIST_NAME + ", COUNT(" + ITEM_ID + ") as " + ITEM_COUNT
      + " FROM " + TodoList.TABLE + " AS " + ALIAS_LIST
      + " LEFT OUTER JOIN " + TodoItem.TABLE + " AS " + ALIAS_ITEM + " ON " + LIST_ID + " = " + ITEM_LIST_ID
      + " GROUP BY " + LIST_ID;

  abstract long id();
  abstract String name();
  abstract int itemCount();

  static Func1<Cursor, List<ListsItem>> MAP = new Func1<Cursor, List<ListsItem>>() {
    @Override public List<ListsItem> call(final Cursor cursor) {
      try {
        List<ListsItem> values = new ArrayList<>(cursor.getCount());
        while (cursor.moveToNext()) {
          long id = Db.getLong(cursor, TodoList.ID);
          String name = Db.getString(cursor, TodoList.NAME);
          int itemCount = Db.getInt(cursor, ITEM_COUNT);
          values.add(new AutoParcel_ListsItem(id, name, itemCount));
        }
        return values;
      } finally {
        cursor.close();
      }
    }
  };
}
