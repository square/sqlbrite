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
