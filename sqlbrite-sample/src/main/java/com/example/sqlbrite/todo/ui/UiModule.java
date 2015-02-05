package com.example.sqlbrite.todo.ui;

import dagger.Module;

@Module(
    injects = {
        ItemsFragment.class,
        ListsFragment.class,
        NewItemFragment.class,
        NewListFragment.class
    },
    complete = false,
    library = true
)
public final class UiModule {
}
