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
package c5db;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.rules.TestName;

public class ScanMatchers {

  public static Matcher<? super Result> isWellFormedUserTable(TestName testName) {
    return new BaseMatcher<Result>() {
      @Override
      public boolean matches(Object o) {
        if (o == null) {
          return false;
        }
        Result result = (Result) o;
        HRegionInfo regioninfo;
        try {
          regioninfo = HRegionInfo.parseFrom(result.getValue(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER));
        } catch (DeserializationException e) {
          e.printStackTrace();
          return false;
        }
        return regioninfo.getRegionNameAsString().startsWith("c5:" + testName.getMethodName());
      }

      @Override
      public void describeTo(Description description) {

      }
    };
  }

}
