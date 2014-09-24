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

package c5db;/*
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

import c5db.interfaces.server.CommandRpcRequest;
import c5db.messages.generated.ModuleSubCommand;
import c5db.messages.generated.ModuleType;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

public class CommandMatchers {

  public static Matcher<CommandRpcRequest<?>> hasMessageWithRPC(String s) {
    return new MessageMatcher(s);
  }

  private static class MessageMatcher extends TypeSafeMatcher<CommandRpcRequest<?>> {
    private final String s;

    public MessageMatcher(String s) {
      this.s = s;
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("a command which is: ").appendValue(s);
    }

    @Override
    protected boolean matchesSafely(CommandRpcRequest<?> commandRpcRequest) {
      if (commandRpcRequest.message instanceof ModuleSubCommand) {
        ModuleSubCommand moduleSubCommand = (ModuleSubCommand) commandRpcRequest.message;

        return moduleSubCommand.getModule().equals(ModuleType.Tablet) &&
            moduleSubCommand.getSubCommand().startsWith(C5ServerConstants.START_META) &&
            moduleSubCommand.getSubCommand().contains(s);
      } else {
        return false;
      }
    }
  }
}
