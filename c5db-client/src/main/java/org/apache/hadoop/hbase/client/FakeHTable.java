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
 *
 *  This file incorporates work covered by the following copyright and
 *  permission notice:
 */
package org.apache.hadoop.hbase.client;

import c5db.client.C5AsyncDatabase;
import c5db.client.C5Constants;
import c5db.client.ProtobufUtil;
import c5db.client.RequestConverter;
import c5db.client.TableInterface;
import c5db.client.generated.ByteArrayComparable;
import c5db.client.generated.Comparator;
import c5db.client.generated.CompareType;
import c5db.client.generated.Condition;
import c5db.client.generated.GetRequest;
import c5db.client.generated.MultiRequest;
import c5db.client.generated.MutateRequest;
import c5db.client.generated.MutationProto;
import c5db.client.generated.RegionAction;
import c5db.client.generated.RegionSpecifier;
import c5db.client.generated.ScanRequest;
import c5db.client.scanner.ClientScannerManager;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;
import io.protostuff.ByteString;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;


/**
 * The main client entry point for putting data into C5. Equivalent to HTablet from HBase.
 */
public class FakeHTable extends c5db.client.FakeHTable {
  public FakeHTable(String hostname, int port, ByteString tableName) throws IOException, InterruptedException, TimeoutException, ExecutionException {
    super(hostname, port, tableName);
  }
}
