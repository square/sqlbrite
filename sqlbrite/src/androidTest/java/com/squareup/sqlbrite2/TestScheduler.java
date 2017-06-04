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
package com.squareup.sqlbrite2;

import io.reactivex.Scheduler;
import io.reactivex.annotations.NonNull;
import io.reactivex.disposables.Disposable;
import java.util.concurrent.TimeUnit;

final class TestScheduler extends Scheduler {
  private final io.reactivex.schedulers.TestScheduler delegate =
      new io.reactivex.schedulers.TestScheduler();

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

    @Override
    public Disposable schedule(@NonNull Runnable run, long delay, @NonNull TimeUnit unit) {
      Disposable disposable = delegateWorker.schedule(run, delay, unit);
      if (runTasksImmediately) {
        triggerActions();
      }
      return disposable;
    }

    @Override public void dispose() {
      delegateWorker.dispose();
    }

    @Override public boolean isDisposed() {
      return delegateWorker.isDisposed();
    }
  }
}
