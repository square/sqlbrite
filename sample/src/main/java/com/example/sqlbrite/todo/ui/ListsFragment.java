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

import android.app.Activity;
import android.os.Bundle;
import android.support.annotation.Nullable;
import android.support.v4.app.Fragment;
import android.support.v4.view.MenuItemCompat;
import android.view.LayoutInflater;
import android.view.Menu;
import android.view.MenuInflater;
import android.view.MenuItem;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ListView;
import butterknife.ButterKnife;
import butterknife.InjectView;
import butterknife.OnItemClick;
import com.example.sqlbrite.todo.R;
import com.example.sqlbrite.todo.TodoApp;
import com.squareup.sqlbrite.BriteDatabase;
import javax.inject.Inject;
import rx.Subscription;
import rx.android.schedulers.AndroidSchedulers;

import static android.support.v4.view.MenuItemCompat.SHOW_AS_ACTION_IF_ROOM;
import static android.support.v4.view.MenuItemCompat.SHOW_AS_ACTION_WITH_TEXT;

public final class ListsFragment extends Fragment {
  interface Listener {
    void onListClicked(long id);
    void onNewListClicked();
  }

  static ListsFragment newInstance() {
    return new ListsFragment();
  }

  @Inject BriteDatabase db;

  @InjectView(android.R.id.list) ListView listView;
  @InjectView(android.R.id.empty) View emptyView;

  private Listener listener;
  private ListsAdapter adapter;
  private Subscription subscription;

  @Override public void onAttach(Activity activity) {
    if (!(activity instanceof Listener)) {
      throw new IllegalStateException("Activity must implement fragment Listener.");
    }

    super.onAttach(activity);
    TodoApp.getComponent(activity).inject(this);
    setHasOptionsMenu(true);

    listener = (Listener) activity;
    adapter = new ListsAdapter(activity);
  }

  @Override public void onCreateOptionsMenu(Menu menu, MenuInflater inflater) {
    super.onCreateOptionsMenu(menu, inflater);

    MenuItem item = menu.add(R.string.new_list)
        .setOnMenuItemClickListener(new MenuItem.OnMenuItemClickListener() {
          @Override public boolean onMenuItemClick(MenuItem item) {
            listener.onNewListClicked();
            return true;
          }
        });
    MenuItemCompat.setShowAsAction(item, SHOW_AS_ACTION_IF_ROOM | SHOW_AS_ACTION_WITH_TEXT);
  }

  @Override public View onCreateView(LayoutInflater inflater, @Nullable ViewGroup container,
      @Nullable Bundle savedInstanceState) {
    return inflater.inflate(R.layout.lists, container, false);
  }

  @Override
  public void onViewCreated(View view, @Nullable Bundle savedInstanceState) {
    super.onViewCreated(view, savedInstanceState);
    ButterKnife.inject(this, view);
    listView.setEmptyView(emptyView);
    listView.setAdapter(adapter);
  }

  @OnItemClick(android.R.id.list) void listClicked(long listId) {
    listener.onListClicked(listId);
  }

  @Override public void onResume() {
    super.onResume();

    getActivity().setTitle("To-Do");

    subscription = db.createQuery(ListsItem.TABLES, ListsItem.QUERY)
        .mapToList(ListsItem.MAPPER)
        .observeOn(AndroidSchedulers.mainThread())
        .subscribe(adapter);
  }

  @Override public void onPause() {
    super.onPause();
    subscription.unsubscribe();
  }
}
