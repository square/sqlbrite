package com.example.sqlbrite.todo.ui;

import android.os.Bundle;
import android.support.v4.app.FragmentActivity;
import com.example.sqlbrite.todo.R;

public final class MainActivity extends FragmentActivity
    implements ListsFragment.Listener, ItemsFragment.Listener {

  @Override protected void onCreate(Bundle savedInstanceState) {
    super.onCreate(savedInstanceState);
    if (savedInstanceState == null) {
      getSupportFragmentManager().beginTransaction()
          .add(android.R.id.content, ListsFragment.newInstance())
          .commit();
    }
  }

  @Override public void onListClicked(long id) {
    getSupportFragmentManager().beginTransaction()
        .setCustomAnimations(R.anim.slide_in_right, R.anim.slide_out_left, R.anim.slide_in_left,
            R.anim.slide_out_right)
        .replace(android.R.id.content, ItemsFragment.newInstance(id))
        .addToBackStack(null)
        .commit();
  }

  @Override public void onNewListClicked() {
    NewListFragment.newInstance().show(getSupportFragmentManager(), "new-list");
  }

  @Override public void onNewItemClicked(long listId) {
    NewItemFragment.newInstance(listId).show(getSupportFragmentManager(), "new-item");
  }
}
