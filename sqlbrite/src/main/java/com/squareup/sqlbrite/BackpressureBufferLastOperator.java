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
package com.squareup.sqlbrite;

import rx.Observable.Operator;
import rx.Producer;
import rx.Subscriber;

/** An operator which keeps the last emitted instance when backpressure has been applied. */
final class BackpressureBufferLastOperator<T> implements Operator<T, T> {
  static final Operator<Object, Object> instance = new BackpressureBufferLastOperator<>();

  static <T> Operator<T, T> instance() {
    //noinspection unchecked
    return (Operator<T, T>) instance;
  }

  private BackpressureBufferLastOperator() {
  }

  @Override public Subscriber<? super T> call(final Subscriber<? super T> child) {
    BufferLastSubscriber<T> parent = new BufferLastSubscriber<>(child);
    child.add(parent);
    child.setProducer(parent.producer);
    return parent;
  }

  static final class BufferLastSubscriber<T> extends Subscriber<T> {
    private static final Object NONE = new Object();

    private final Subscriber<? super T> child;

    private volatile Object last = NONE; // Guarded by 'this'.
    private volatile long requested; // Guarded by 'this'. Starts at zero.

    final Producer producer = new Producer() {
      @Override public void request(long n) {
        if (n < 0) {
          throw new IllegalArgumentException("requested " + n + " < 0");
        }
        if (n == 0) {
          return;
        }

        Object candidate;
        synchronized (BufferLastSubscriber.this) {
          candidate = last;

          long currentRequested = requested;
          if (Long.MAX_VALUE - n <= currentRequested) {
            requested = Long.MAX_VALUE;
          } else {
            if (candidate != NONE) {
              n--; // Decrement since we will be emitting a value.
            }
            requested = currentRequested + n;
          }
        }

        // Only emit if the value is not the explicit NONE marker.
        if (candidate != NONE) {
          //noinspection unchecked
          child.onNext((T) candidate);
        }
      }
    };

    public BufferLastSubscriber(Subscriber<? super T> child) {
      this.child = child;
    }

    @Override public void onNext(T t) {
      boolean emit = false;
      synchronized (this) {
        long currentRequested = requested;
        if (currentRequested == Long.MAX_VALUE) {
          // No need to decrement when the firehose is open.
          emit = true;
        } else if (currentRequested > 0) {
          requested = currentRequested - 1;
          emit = true;
        } else {
          last = t; // Not emitting, store for later.
        }
      }

      if (emit) {
        child.onNext(t);
      }
    }

    @Override public void onStart() {
      request(Long.MAX_VALUE);
    }

    @Override public void onCompleted() {
      child.onCompleted();
    }

    @Override public void onError(Throwable e) {
      child.onError(e);
    }
  }
}
