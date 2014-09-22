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
package c5db.interfaces.tablet;

/**
* Notification about a tablet changing state.
*/
public class TabletStateChange {
  public final Tablet tablet;
  public final Tablet.State state;
  public final Throwable optError;

  public TabletStateChange(Tablet tablet, Tablet.State state, Throwable optError) {
    this.tablet = tablet;
    this.state = state;
    this.optError = optError;
  }

  @Override
  public String toString() {
    return "TabletStateChange{" +
        "tablet=" + tablet +
        ", state=" + state +
        ", optError=" + optError +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TabletStateChange that = (TabletStateChange) o;

    if (optError != null ? !optError.equals(that.optError) : that.optError != null) {
      return false;
    }
    return state == that.state && tablet.equals(that.tablet);

  }

  @Override
  public int hashCode() {
    int result = tablet.hashCode();
    result = 31 * result + state.hashCode();
    result = 31 * result + (optError != null ? optError.hashCode() : 0);
    return result;
  }
}
