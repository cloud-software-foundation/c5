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

package matchers;

import c5db.interfaces.tablet.Tablet;
import c5db.interfaces.tablet.TabletStateChange;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

/**
 * Matchers for instances of {@link c5db.interfaces.tablet.TabletStateChange}
 */
public class TabletMatchers {
  public static Matcher<TabletStateChange> hasMessageWithState(Tablet.State state) {
    return new StateMatcher(state);
  }

  public static class StateMatcher extends TypeSafeMatcher<TabletStateChange> {

    private final Tablet.State state;

    public StateMatcher(Tablet.State state) {
      this.state = state;
    }

    @Override
    protected boolean matchesSafely(TabletStateChange item) {
      return item.state.equals(state);
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("a state that is")
          .appendValue(state);
    }
  }
}
