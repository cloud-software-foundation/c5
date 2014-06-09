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
package c5db.client.scanner;

import c5db.client.ProtobufUtil;
import org.apache.hadoop.hbase.client.AbstractClientScanner;
import org.apache.hadoop.hbase.client.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

/**
 * Mostly an HBase-ism. Handles scanner logic for htable users.
 */
public class ClientScanner extends AbstractClientScanner {
  private static final Logger LOG = LoggerFactory.getLogger(ClientScanner.class);

  private final C5ClientScanner c5ClientScanner;

  public ClientScanner(C5ClientScanner c5ClientScanner) {
    this.c5ClientScanner = c5ClientScanner;
  }

  @Override
  public Result next() throws IOException {
    try {
      return ProtobufUtil.toResult(c5ClientScanner.next());
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public Result[] next(int nbRows) throws IOException {

    try {
      c5db.client.generated.Result[] protoResults = c5ClientScanner.next(nbRows);
      return Arrays.asList(protoResults).stream().map(ProtobufUtil::toResult).toArray(Result[]::new);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void close() {
    try {
      c5ClientScanner.close();
    } catch (InterruptedException e) {
      LOG.error("Unable to close without an error");
      e.printStackTrace();
    }
  }
}
