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
package c5db.webadmin;

import c5db.C5ServerConstants;
import c5db.interfaces.C5Server;
import c5db.interfaces.server.CommandRpcRequest;
import c5db.messages.generated.ModuleSubCommand;
import c5db.messages.generated.ModuleType;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import sun.misc.BASE64Encoder;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Create a table for us
 */
@WebServlet(urlPatterns = {"/"})
public class CreateTableServlet extends HttpServlet {
  private final C5Server server;

  public CreateTableServlet(C5Server server) {

    this.server = server;
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse response) throws ServletException, IOException {
    // only parameter that matters: table-name
    String tableNameFromParam = req.getParameter("tablename");

    System.out.println("SERVLET: CREATE TABLE!" + tableNameFromParam);


    response.setContentType("text/plain");

    if (tableNameFromParam == null || tableNameFromParam.equals("")) {
      response.getWriter().printf("BAD_TABLE_NAME");
      response.getWriter().flush();
      return;
    }

    // build create table command:
    TableName tableName = TableName.valueOf(tableNameFromParam);
    HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
    tableDescriptor.addFamily(new HColumnDescriptor("cf"));
    HRegionInfo regionInfo = new HRegionInfo(tableName, new byte[]{0}, new byte[]{}, false, 1);
    String peerString = String.valueOf(server.getNodeId());

    BASE64Encoder base64Encoder = new BASE64Encoder();

    String strTableDesc = base64Encoder.encodeBuffer(tableDescriptor.toByteArray());
    String strRegionInfo = base64Encoder.encodeBuffer(regionInfo.toByteArray());

    String commandString = C5ServerConstants.CREATE_TABLE + ":" + strTableDesc + "," +
        strRegionInfo + "," + peerString;

    CommandRpcRequest<ModuleSubCommand> commandRequest = new CommandRpcRequest<>(server.getNodeId(),
        new ModuleSubCommand(ModuleType.Tablet,
            commandString));

    server.getCommandChannel().publish(commandRequest);

    response.getWriter().printf("OK");
    response.getWriter().flush();
  }
}
