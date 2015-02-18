package com.example.sqlbrite.todo.db;

import android.app.Application;
import android.database.sqlite.SQLiteOpenHelper;
import com.example.sqlbrite.todo.BuildConfig;
import com.squareup.sqlbrite.SqlBrite;
import dagger.Module;
import dagger.Provides;
import javax.inject.Singleton;
import timber.log.Timber;

@Module(complete = false, library = true)
public final class DbModule {
  @Provides @Singleton SQLiteOpenHelper provideOpenHelper(Application application) {
    return new DbOpenHelper(application);
  }

  @Provides @Singleton SqlBrite provideSqlBrite(SQLiteOpenHelper openHelper) {
    SqlBrite db = SqlBrite.create(openHelper);

    if (BuildConfig.DEBUG) {
      db.setLogger(new SqlBrite.Logger() {
        @Override public void log(String message) {
          Timber.tag("Database").v(message);
        }
      });
      db.setLoggingEnabled(true);
    }

    return db;
  }
}
