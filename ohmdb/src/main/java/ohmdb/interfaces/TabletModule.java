/*
 * Copyright (C) 2013  Ohm Data
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
package ohmdb.interfaces;


import ohmdb.messages.ControlMessages;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.jetlang.channels.Channel;

/**
 * Manager of tablets (aka regions)
 */
@DependsOn(ReplicationModule.class)
@ModuleTypeBinding(ControlMessages.ModuleType.Tablet)
public interface TabletModule extends OhmModule {
    public Channel<TabletStateChange> getTabletStateChanges();

    public static class TabletStateChange {
        public final HRegion tablet;
        public final int state;
        public final Throwable optError;

        @Override
        public String toString() {
            return "TabletStateChange{" +
                    "tablet=" + tablet +
                    ", state=" + state +
                    ", optError=" + optError +
                    '}';
        }

        public TabletStateChange(HRegion tablet, int state, Throwable optError) {
            this.tablet = tablet;
            this.state = state;
            this.optError = optError;
        }
    }
}
