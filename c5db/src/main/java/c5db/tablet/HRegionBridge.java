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

package c5db.tablet;

import c5db.client.generated.Condition;
import c5db.client.generated.MutationProto;
import c5db.regionserver.ReverseProtobufUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Bridge between the (complex) HRegion and the rest of c5.
 * <p/>
 * Provides an abstraction and test point, and lessons in how to abstract
 * and extract HRegion functionality.
 */
public class HRegionBridge implements Region {
  private static final Logger LOG = LoggerFactory.getLogger(HRegionBridge.class);

  private HRegion theRegion;

  public HRegionBridge(final HRegion theRegion) {
    this.theRegion = theRegion;
  }


  @Override
  public boolean mutate(MutationProto mutation, Condition condition) throws IOException {
    final MutationProto.MutationType type = mutation.getMutateType();
    boolean success;
    switch (type) {
      case PUT:
        if (condition == null || condition.getRow() == null) {
          success = simplePut(mutation);
        } else {
          success = checkAndPut(mutation, condition);
        }
        break;
      case DELETE:
        if (condition == null || condition.getRow() == null) {
          success = simpleDelete(mutation);
        } else {
          success = checkAndDelete(mutation, condition);
        }
        break;
      default:
        throw new IOException("mutate supports atomic put and/or delete, not " + type.name());
    }
    return success;
  }

  private boolean checkAndPut(MutationProto mutation, Condition condition) throws IOException {
    boolean success;
    final byte[] row = condition.getRow().array();
    final byte[] cf = condition.getFamily().array();
    final byte[] cq = condition.getQualifier().array();

    final CompareFilter.CompareOp compareOp = CompareFilter.CompareOp.valueOf(condition.getCompareType().name());
    final ByteArrayComparable comparator = ReverseProtobufUtil.toComparator(condition.getComparator());

    success = this.getTheRegion().checkAndMutate(row,
        cf,
        cq,
        compareOp,
        comparator,
        ReverseProtobufUtil.toPut(mutation),
        true);
    return success;
  }

  private boolean simplePut(MutationProto mutation) {
    try {
      this.getTheRegion().put(ReverseProtobufUtil.toPut(mutation));
    } catch (IOException e) {
      LOG.error(e.getLocalizedMessage());
      return false;
    }
    return true;
  }


  private boolean checkAndDelete(MutationProto mutation, Condition condition) throws IOException {
    boolean success;
    final byte[] row = condition.getRow().array();
    final byte[] cf = condition.getFamily().array();
    final byte[] cq = condition.getQualifier().array();

    final CompareFilter.CompareOp compareOp = CompareFilter.CompareOp.valueOf(condition.getCompareType().name());
    final ByteArrayComparable comparator = ReverseProtobufUtil.toComparator(condition.getComparator());

    success = this.getTheRegion().checkAndMutate(row,
        cf,
        cq,
        compareOp,
        comparator,
        ReverseProtobufUtil.toDelete(mutation),
        true);
    return success;
  }

  private boolean simpleDelete(MutationProto mutation) {
    try {
      this.getTheRegion().delete(ReverseProtobufUtil.toDelete(mutation));
    } catch (IOException e) {
      LOG.error(e.getLocalizedMessage());
      return false;
    }
    return true;
  }

  @Override
  public HRegion getTheRegion() {
    return theRegion;
  }

  @Override
  public RegionScanner getScanner(Scan scan) {
    try {
      return getTheRegion().getScanner(scan);
    } catch (IOException e) {
      e.printStackTrace();

    }
    return null;
  }

  @Override
  public boolean exists(c5db.client.generated.Get get) throws IOException {
    final org.apache.hadoop.hbase.client.Get serverGet = ReverseProtobufUtil.toGet(get);
    Result result = this.getTheRegion().get(serverGet);
    return result.getExists();
  }

  @Override
  public c5db.client.generated.Result get(c5db.client.generated.Get get) throws IOException {
    final org.apache.hadoop.hbase.client.Get serverGet = ReverseProtobufUtil.toGet(get);
    return ReverseProtobufUtil.toResult(this.getTheRegion().get(serverGet));
  }
}
