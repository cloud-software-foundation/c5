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

/**
 * Created with IntelliJ IDEA.
 * User: alex
 * Date: 5/22/13
 * Time: 12:26 AM
 */
package c5db;

import c5db.client.generated.Call;
import c5db.client.generated.Response;
import org.junit.Test;

public class TestProto {
  @Test
  public void testClientProtoCall() {
    Call call = Call.getDefaultInstance();
  }

  @Test
  public void testClientProtoResponse() {
    Response response = Response.getDefaultInstance();
  }

}
