/*
 * Copyright (C) 2017 Square, Inc.
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
@file:Suppress("NOTHING_TO_INLINE") // Extensions provided for intentional convenience.

package com.squareup.sqlbrite3

import android.database.Cursor
import com.squareup.sqlbrite3.SqlBrite.Query
import io.reactivex.Observable
import java.util.Optional

typealias Mapper<T> = (Cursor) -> T

/**
 * Transforms an observable of single-row [Query] to an observable of `T` using `mapper`.
 *
 * It is an error for a query to pass through this operator with more than 1 row in its result set.
 * Use `LIMIT 1` on the underlying SQL query to prevent this. Result sets with 0 rows do not emit
 * an item.
 *
 * This operator ignores null cursors returned from [Query.run].
 *
 * @param mapper Maps the current [Cursor] row to `T`. May not return null.
 */
inline fun <T> Observable<Query>.mapToOne(noinline mapper: Mapper<T>): Observable<T>
    = lift(Query.mapToOne(mapper))

/**
 * Transforms an observable of single-row [Query] to an observable of `T` using `mapper`
 *
 * It is an error for a query to pass through this operator with more than 1 row in its result set.
 * Use `LIMIT 1` on the underlying SQL query to prevent this. Result sets with 0 rows emit
 * `default`.
 *
 * This operator emits `defaultValue` if null is returned from [Query.run].
 *
 * @param mapper Maps the current [Cursor] row to `T`. May not return null.
 * @param default Value returned if result set is empty
 */
inline fun <T> Observable<Query>.mapToOneOrDefault(default: T, noinline mapper: Mapper<T>): Observable<T>
    = lift(Query.mapToOneOrDefault(mapper, default))

/**
 * Transforms an observable of single-row [Query] to an observable of `T` using `mapper.
 *
 * It is an error for a query to pass through this operator with more than 1 row in its result set.
 * Use `LIMIT 1` on the underlying SQL query to prevent this. Result sets with 0 rows emit
 * `default`.
 *
 * This operator ignores null cursors returned from [Query.run].
 *
 * @param mapper Maps the current [Cursor] row to `T`. May not return null.
 */
inline fun <T> Observable<Query>.mapToOptional(noinline mapper: Mapper<T>): Observable<Optional<T>>
    = lift(Query.mapToOptional(mapper))

/**
 * Transforms an observable of [Query] to `List<T>` using `mapper` for each row.
 *
 * Be careful using this operator as it will always consume the entire cursor and create objects
 * for each row, every time this observable emits a new query. On tables whose queries update
 * frequently or very large result sets this can result in the creation of many objects.
 *
 * This operator ignores null cursors returned from [Query.run].
 *
 * @param mapper Maps the current [Cursor] row to `T`. May not return null.
 */
inline fun <T> Observable<Query>.mapToList(noinline mapper: Mapper<T>): Observable<List<T>>
    = lift(Query.mapToList(mapper))
