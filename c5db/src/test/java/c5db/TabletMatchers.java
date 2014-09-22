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

package c5db;

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
