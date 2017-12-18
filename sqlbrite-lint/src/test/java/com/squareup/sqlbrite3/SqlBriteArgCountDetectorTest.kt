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
package com.squareup.sqlbrite3

import com.android.tools.lint.checks.infrastructure.TestFiles.java
import com.android.tools.lint.checks.infrastructure.TestLintTask.lint
import org.junit.Test

class SqlBriteArgCountDetectorTest {

  companion object {
    private val BRITE_DATABASE_STUB = java(
        """
      package com.squareup.sqlbrite3;

      public final class BriteDatabase {

        public void query(String sql, Object... args) {
        }

        public void createQuery(String table, String sql, Object... args) {
        }

        // simulate createQuery with SupportSQLiteQuery query parameter
        public void createQuery(String table, int something) {
        }
      }
      """.trimIndent()
    )
  }

  @Test
  fun cleanCaseWithWithQueryAsLiteral() {
    lint().files(
        BRITE_DATABASE_STUB,
        java(
            """
              package test.pkg;

              import com.squareup.sqlbrite3.BriteDatabase;

              public class Test {
                  private static final String QUERY = "SELECT name FROM table WHERE id = ?";

                  public void test() {
                    BriteDatabase db = new BriteDatabase();
                    db.query(QUERY, "id");
                  }

              }
            """.trimIndent()))
        .issues(SqlBriteArgCountDetector.ISSUE)
        .run()
        .expectClean()
  }

  @Test
  fun cleanCaseWithQueryAsBinaryExpression() {
    lint().files(
        BRITE_DATABASE_STUB,
        java(
            """
              package test.pkg;

              import com.squareup.sqlbrite3.BriteDatabase;

              public class Test {
                  private static final String QUERY = "SELECT name FROM table WHERE ";

                  public void test() {
                    BriteDatabase db = new BriteDatabase();
                    db.query(QUERY + "id = ?", "id");
                  }

              }
            """.trimIndent()))
        .issues(SqlBriteArgCountDetector.ISSUE)
        .run()
        .expectClean()
  }

  @Test
  fun cleanCaseWithQueryThatCantBeEvaluated() {
    lint().files(
        BRITE_DATABASE_STUB,
        java(
            """
              package test.pkg;

              import com.squareup.sqlbrite3.BriteDatabase;

              public class Test {
                  private static final String QUERY = "SELECT name FROM table WHERE id = ?";

                  public void test() {
                    BriteDatabase db = new BriteDatabase();
                    db.query(query(), "id");
                  }

                  private String query() {
                    return QUERY + " age = ?";
                  }

              }
            """.trimIndent()))
        .issues(SqlBriteArgCountDetector.ISSUE)
        .run()
        .expectClean()
  }

  @Test
  fun cleanCaseWithNonVarargMethodCall() {
    lint().files(
        BRITE_DATABASE_STUB,
        java(
            """
              package test.pkg;

              import com.squareup.sqlbrite3.BriteDatabase;

              public class Test {

                  public void test() {
                    BriteDatabase db = new BriteDatabase();
                    db.createQuery("table", 42);
                  }

              }
            """.trimIndent()))
        .issues(SqlBriteArgCountDetector.ISSUE)
        .run()
        .expectClean()
  }

  @Test
  fun queryMethodWithWrongNumberOfArguments() {
    lint().files(
        BRITE_DATABASE_STUB,
        java(
            """
              package test.pkg;

              import com.squareup.sqlbrite3.BriteDatabase;

              public class Test {
                  private static final String QUERY = "SELECT name FROM table WHERE id = ?";

                  public void test() {
                    BriteDatabase db = new BriteDatabase();
                    db.query(QUERY);
                  }

              }
            """.trimIndent()))
        .issues(SqlBriteArgCountDetector.ISSUE)
        .run()
        .expect("src/test/pkg/Test.java:10: " +
            "Error: Wrong argument count, query SELECT name FROM table WHERE id = ?" +
            " requires 1 argument, but was provided 0 arguments [SqlBriteArgCount]\n" +
            "      db.query(QUERY);\n" +
            "      ~~~~~~~~~~~~~~~\n" +
            "1 errors, 0 warnings")
  }

  @Test
  fun createQueryMethodWithWrongNumberOfArguments() {
    lint().files(
        BRITE_DATABASE_STUB,
        java(
            """
              package test.pkg;

              import com.squareup.sqlbrite3.BriteDatabase;

              public class Test {
                  private static final String QUERY = "SELECT name FROM table WHERE id = ?";

                  public void test() {
                    BriteDatabase db = new BriteDatabase();
                    db.createQuery("table", QUERY);
                  }

              }
            """.trimIndent()))
        .issues(SqlBriteArgCountDetector.ISSUE)
        .run()
        .expect("src/test/pkg/Test.java:10: " +
            "Error: Wrong argument count, query SELECT name FROM table WHERE id = ?" +
            " requires 1 argument, but was provided 0 arguments [SqlBriteArgCount]\n" +
            "      db.createQuery(\"table\", QUERY);\n" +
            "      ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n" +
            "1 errors, 0 warnings")
  }
}