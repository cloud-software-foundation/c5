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
