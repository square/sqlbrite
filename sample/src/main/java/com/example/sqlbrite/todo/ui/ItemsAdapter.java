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

import android.content.Context;
import android.text.SpannableString;
import android.text.style.StrikethroughSpan;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.BaseAdapter;
import android.widget.CheckedTextView;
import com.example.sqlbrite.todo.db.TodoItem;
import io.reactivex.functions.Consumer;
import java.util.Collections;
import java.util.List;

final class ItemsAdapter extends BaseAdapter implements Consumer<List<TodoItem>> {
  private final LayoutInflater inflater;

  private List<TodoItem> items = Collections.emptyList();

  public ItemsAdapter(Context context) {
    inflater = LayoutInflater.from(context);
  }

  @Override public void accept(List<TodoItem> items) {
    this.items = items;
    notifyDataSetChanged();
  }

  @Override public int getCount() {
    return items.size();
  }

  @Override public TodoItem getItem(int position) {
    return items.get(position);
  }

  @Override public long getItemId(int position) {
    return getItem(position).id();
  }

  @Override public boolean hasStableIds() {
    return true;
  }

  @Override public View getView(int position, View convertView, ViewGroup parent) {
    if (convertView == null) {
      convertView = inflater.inflate(android.R.layout.simple_list_item_multiple_choice, parent, false);
    }

    TodoItem item = getItem(position);
    CheckedTextView textView = (CheckedTextView) convertView;
    textView.setChecked(item.complete());

    CharSequence description = item.description();
    if (item.complete()) {
      SpannableString spannable = new SpannableString(description);
      spannable.setSpan(new StrikethroughSpan(), 0, description.length(), 0);
      description = spannable;
    }

    textView.setText(description);

    return convertView;
  }
}
