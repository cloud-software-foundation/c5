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
