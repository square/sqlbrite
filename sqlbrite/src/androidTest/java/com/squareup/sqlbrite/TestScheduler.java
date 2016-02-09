/*
 * Copyright (C) 2016 Square, Inc.
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

import java.util.concurrent.TimeUnit;
import rx.Scheduler;
import rx.Subscription;
import rx.functions.Action0;

final class TestScheduler extends Scheduler {
  private final rx.schedulers.TestScheduler delegate = new rx.schedulers.TestScheduler();
  private boolean runTasksImmediately = true;

  public void runTasksImmediately(boolean runTasksImmediately) {
    this.runTasksImmediately = runTasksImmediately;
  }

  public void triggerActions() {
    delegate.triggerActions();
  }

  @Override public Worker createWorker() {
    return new TestWorker();
  }

  class TestWorker extends Worker {
    private final Worker delegateWorker = delegate.createWorker();

    @Override public Subscription schedule(Action0 action) {
      Subscription subscription = delegateWorker.schedule(action);
      if (runTasksImmediately) {
        triggerActions();
      }
      return subscription;
    }

    @Override public Subscription schedule(Action0 action, long delayTime, TimeUnit unit) {
      Subscription subscription = delegateWorker.schedule(action, delayTime, unit);
      if (runTasksImmediately) {
        triggerActions();
      }
      return subscription;
    }

    @Override public void unsubscribe() {
      delegateWorker.unsubscribe();
    }

    @Override public boolean isUnsubscribed() {
      return delegateWorker.isUnsubscribed();
    }
  }
}
