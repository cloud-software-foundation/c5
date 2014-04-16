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

package c5db;

import c5db.interfaces.TabletModule;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

/**
 * Matchers for instances of {@link c5db.interfaces.TabletModule.TabletStateChange}
 */
public class TabletMatchers {
  public static Matcher<TabletModule.TabletStateChange> hasMessageWithState(TabletModule.Tablet.State state) {
    return new StateMatcher(state);
  }

  public static class StateMatcher extends TypeSafeMatcher<TabletModule.TabletStateChange> {

    private final TabletModule.Tablet.State state;

    public StateMatcher(TabletModule.Tablet.State state) {
      this.state = state;
    }

    @Override
    protected boolean matchesSafely(TabletModule.TabletStateChange item) {
      return item.state.equals(state);
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("a state that is")
          .appendValue(state);
    }
  }
}
