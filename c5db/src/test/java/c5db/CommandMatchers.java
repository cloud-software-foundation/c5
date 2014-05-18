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

/**
 * Created by posix4e on 5/18/14.
 */
public class CommandMatchers {

  public static Matcher<CommandRpcRequest<ModuleSubCommand>> hasMessageWithRPC(String s) {
    return new MessageMatcher(s);
  }

  private static class MessageMatcher extends TypeSafeMatcher<CommandRpcRequest<ModuleSubCommand>> {
    private final String s;

    public MessageMatcher(String s) {
      this.s = s;
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("a command which is: ").appendValue(s);
    }

    @Override
    protected boolean matchesSafely(CommandRpcRequest<ModuleSubCommand> moduleSubCommandCommandRpcRequest) {
      return moduleSubCommandCommandRpcRequest.message.getModule().equals(ModuleType.Tablet) &&
          moduleSubCommandCommandRpcRequest.message.getSubCommand().startsWith(C5ServerConstants.START_META) &&
          moduleSubCommandCommandRpcRequest.message.getSubCommand().contains(s);

    }
  }
}
