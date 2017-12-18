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

import com.android.tools.lint.detector.api.Category
import com.android.tools.lint.detector.api.ConstantEvaluator.evaluateString
import com.android.tools.lint.detector.api.Detector
import com.android.tools.lint.detector.api.Implementation
import com.android.tools.lint.detector.api.Issue
import com.android.tools.lint.detector.api.JavaContext
import com.android.tools.lint.detector.api.Scope.JAVA_FILE
import com.android.tools.lint.detector.api.Scope.TEST_SOURCES
import com.android.tools.lint.detector.api.Severity
import com.intellij.psi.PsiMethod
import org.jetbrains.uast.UCallExpression
import java.util.EnumSet

private const val BRITE_DATABASE = "com.squareup.sqlbrite3.BriteDatabase"
private const val QUERY_METHOD_NAME = "query"
private const val CREATE_QUERY_METHOD_NAME = "createQuery"

class SqlBriteArgCountDetector : Detector(), Detector.UastScanner {

  companion object {

    val ISSUE: Issue = Issue.create(
        "SqlBriteArgCount",
        "Number of provided arguments doesn't match number " +
            "of arguments specified in query",
        "When providing arguments to query you need to provide the same amount of " +
            "arguments that is specified in query.",
        Category.MESSAGES,
        9,
        Severity.ERROR,
        Implementation(SqlBriteArgCountDetector::class.java, EnumSet.of(JAVA_FILE, TEST_SOURCES)))
  }

  override fun getApplicableMethodNames() = listOf(CREATE_QUERY_METHOD_NAME, QUERY_METHOD_NAME)

  override fun visitMethod(context: JavaContext, call: UCallExpression, method: PsiMethod) {
    val evaluator = context.evaluator

    if (evaluator.isMemberInClass(method, BRITE_DATABASE)) {
      // Skip non varargs overloads.
      if (!method.isVarArgs) return

      // Position of sql parameter depends on method.
      val sql = evaluateString(context,
          call.valueArguments[if (call.isQueryMethod()) 0 else 1], true) ?: return

      // Count only vararg arguments.
      val argumentsCount = call.valueArgumentCount - if (call.isQueryMethod()) 1 else 2
      val questionMarksCount = sql.count { it == '?' }
      if (argumentsCount != questionMarksCount) {
        val requiredArguments = "$questionMarksCount ${"argument".pluralize(questionMarksCount)}"
        val actualArguments = "$argumentsCount ${"argument".pluralize(argumentsCount)}"
        context.report(ISSUE, call, context.getLocation(call), "Wrong argument count, " +
            "query $sql requires $requiredArguments, but was provided $actualArguments")
      }
    }
  }

  private fun UCallExpression.isQueryMethod() = methodName == QUERY_METHOD_NAME

  private fun String.pluralize(count: Int) = if (count == 1) this else this + "s"
}