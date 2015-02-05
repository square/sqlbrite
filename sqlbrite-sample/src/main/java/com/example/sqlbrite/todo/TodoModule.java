package com.example.sqlbrite.todo;

import android.app.Application;
import com.example.sqlbrite.todo.db.DbModule;
import com.example.sqlbrite.todo.ui.UiModule;
import dagger.Module;
import dagger.Provides;
import javax.inject.Singleton;

@Module(
    includes = {
        DbModule.class,
        UiModule.class
    }
)
public final class TodoModule {
  private final Application application;

  TodoModule(Application application) {
    this.application = application;
  }

  @Provides @Singleton Application provideApplication() {
    return application;
  }
}
