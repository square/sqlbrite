package com.example.sqlbrite.todo.ui;

import android.content.Context;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.BaseAdapter;
import android.widget.TextView;
import java.util.Collections;
import java.util.List;
import rx.functions.Action1;

final class ListsAdapter extends BaseAdapter implements Action1<List<ListsItem>> {
  private final LayoutInflater inflater;

  private List<ListsItem> items = Collections.emptyList();

  public ListsAdapter(Context context) {
    this.inflater = LayoutInflater.from(context);
  }

  @Override public void call(List<ListsItem> items) {
    this.items = items;
    notifyDataSetChanged();
  }

  @Override public int getCount() {
    return items.size();
  }

  @Override public ListsItem getItem(int position) {
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
      convertView = inflater.inflate(android.R.layout.simple_list_item_1, parent, false);
    }

    ListsItem item = getItem(position);
    ((TextView) convertView).setText(item.name() + " (" + item.itemCount() + ")");

    return convertView;
  }
}
