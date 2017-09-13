package com.squareup.sqlbrite3;

import android.database.Cursor;
import android.database.MatrixCursor;
import android.support.annotation.Nullable;
import android.support.test.runner.AndroidJUnit4;
import com.squareup.sqlbrite3.SqlBrite.Query;
import io.reactivex.functions.Function;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import org.junit.runner.RunWith;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.fail;

@RunWith(AndroidJUnit4.class)
@SuppressWarnings("CheckResult")
public final class SqlBriteTest {
  private static final String FIRST_NAME = "first_name";
  private static final String LAST_NAME = "last_name";
  private static final String[] COLUMN_NAMES = { FIRST_NAME, LAST_NAME };

  @Test public void builderDisallowsNull() {
    SqlBrite.Builder builder = new SqlBrite.Builder();
    try {
      builder.logger(null);
      fail();
    } catch (NullPointerException e) {
      assertThat(e).hasMessage("logger == null");
    }
    try {
      builder.queryTransformer(null);
      fail();
    } catch (NullPointerException e) {
      assertThat(e).hasMessage("queryTransformer == null");
    }
  }

  @Test public void asRowsEmpty() {
    MatrixCursor cursor = new MatrixCursor(COLUMN_NAMES);
    Query query = new CursorQuery(cursor);
    List<Name> names = query.asRows(Name.MAP).toList().blockingGet();
    assertThat(names).isEmpty();
  }

  @Test public void asRows() {
    MatrixCursor cursor = new MatrixCursor(COLUMN_NAMES);
    cursor.addRow(new Object[] { "Alice", "Allison" });
    cursor.addRow(new Object[] { "Bob", "Bobberson" });

    Query query = new CursorQuery(cursor);
    List<Name> names = query.asRows(Name.MAP).toList().blockingGet();
    assertThat(names).containsExactly(new Name("Alice", "Allison"), new Name("Bob", "Bobberson"));
  }

  @Test public void asRowsStopsWhenUnsubscribed() {
    MatrixCursor cursor = new MatrixCursor(COLUMN_NAMES);
    cursor.addRow(new Object[] { "Alice", "Allison" });
    cursor.addRow(new Object[] { "Bob", "Bobberson" });

    Query query = new CursorQuery(cursor);
    final AtomicInteger count = new AtomicInteger();
    query.asRows(new Function<Cursor, Name>() {
      @Override public Name apply(Cursor cursor) throws Exception {
        count.incrementAndGet();
        return Name.MAP.apply(cursor);
      }
    }).take(1).blockingFirst();
    assertThat(count.get()).isEqualTo(1);
  }

  @Test public void asRowsEmptyWhenNullCursor() {
    Query nully = new Query() {
      @Nullable @Override public Cursor run() {
        return null;
      }
    };

    final AtomicInteger count = new AtomicInteger();
    nully.asRows(new Function<Cursor, Name>() {
      @Override public Name apply(Cursor cursor) throws Exception {
        count.incrementAndGet();
        return Name.MAP.apply(cursor);
      }
    }).test().assertNoValues().assertComplete();

    assertThat(count.get()).isEqualTo(0);
  }

  static final class Name {
    static final Function<Cursor, Name> MAP = new Function<Cursor, Name>() {
      @Override public Name apply(Cursor cursor) {
        return new Name( //
            cursor.getString(cursor.getColumnIndexOrThrow(FIRST_NAME)),
            cursor.getString(cursor.getColumnIndexOrThrow(LAST_NAME)));
      }
    };

    final String first;
    final String last;

    Name(String first, String last) {
      this.first = first;
      this.last = last;
    }

    @Override public boolean equals(Object o) {
      if (o == this) return true;
      if (!(o instanceof Name)) return false;
      Name other = (Name) o;
      return first.equals(other.first) && last.equals(other.last);
    }

    @Override public int hashCode() {
      return first.hashCode() * 17 + last.hashCode();
    }

    @Override public String toString() {
      return "Name[" + first + ' ' + last + ']';
    }
  }

  static final class CursorQuery extends Query {
    private final Cursor cursor;

    CursorQuery(Cursor cursor) {
      this.cursor = cursor;
    }

    @Override public Cursor run() {
      return cursor;
    }
  }
}
