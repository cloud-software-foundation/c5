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

package c5db.client;


import org.apache.hadoop.hbase.client.Result;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

/**
 * A Hamcrest matcher for results
 */
public class ResultMatcher extends TypeSafeMatcher<Result> {
  private final Result precond;


  public ResultMatcher(Result precond) {
    this.precond = precond;
  }


  @Override
  protected boolean matchesSafely(Result item) {
    return item.listCells().size() == precond.listCells().size()
        && item.getExists() == precond.getExists()
        && item.toString().equals(precond.toString());

  }

  @Override
  public void describeTo(Description description) {
    description.appendText("a result ").appendText(precond.toString());
  }
}
