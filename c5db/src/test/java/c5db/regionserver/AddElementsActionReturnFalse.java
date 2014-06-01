/*
 * Copyright (C) 2014  Ohm Data
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package c5db.regionserver;

import org.hamcrest.Description;
import org.jmock.api.Action;
import org.jmock.api.Invocation;

import java.util.Arrays;
import java.util.Collection;

public class AddElementsActionReturnFalse<T> implements Action {
  private final Collection<T> elements;

  private AddElementsActionReturnFalse(Collection<T> elements) {
    this.elements = elements;
  }

  public static <T> Action addElements(T... newElements) {
    return new AddElementsActionReturnFalse<>(Arrays.asList(newElements));
  }

  public void describeTo(Description description) {
    description.appendText("adds ")
        .appendValueList("", ", ", "", elements)
        .appendText(" to a collection");
  }

  public Object invoke(Invocation invocation) throws Throwable {
    ((Collection<T>) invocation.getParameter(0)).addAll(elements);
    return false;
  }

}