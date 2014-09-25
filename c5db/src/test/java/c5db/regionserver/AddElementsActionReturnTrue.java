/*
 * Copyright 2014 WANdisco
 *
 *  WANdisco licenses this file to you under the Apache License,
 *  version 2.0 (the "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations
 *  under the License.
 */

package c5db.regionserver;

import org.hamcrest.Description;
import org.jmock.api.Action;
import org.jmock.api.Invocation;

import java.util.Arrays;
import java.util.Collection;

public class AddElementsActionReturnTrue<T> implements Action {
  private final Collection<T> elements;

  private AddElementsActionReturnTrue(Collection<T> elements) {
    this.elements = elements;
  }

  public static <T> Action addElements(T... newElements) {
    return new AddElementsActionReturnTrue<>(Arrays.asList(newElements));
  }

  public void describeTo(Description description) {
    description.appendText("adds ")
        .appendValueList("", ", ", "", elements)
        .appendText(" to a collection");
  }

  public Object invoke(Invocation invocation) throws Throwable {
    ((Collection<T>) invocation.getParameter(0)).addAll(elements);
    return true;
  }

}