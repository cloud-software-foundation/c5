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
package c5db;

import com.google.common.primitives.UnsignedBytes;

import java.util.Comparator;

public class C5Compare implements Comparator<byte[]> {
  @Override
  public int compare(byte[] left, byte[] right) {
    return compareTo(left, right);
  }

  private static boolean isEmpty(byte[] left) {
    return left.length == 0;
  }

  private static int compareTo(byte[] left, byte[] right) {
    if (isEmpty(left)) {
      if (isEmpty(right)) {
        return 0;
      }
      return 1;
    } else if (isEmpty(right)) {
      return -1;
    }

    int minLength = Math.min(left.length, right.length);
    for (int i = 0; i < minLength; i++) {
      int result = UnsignedBytes.compare(left[i], right[i]);
      if (result != 0) {
        return result;
      }
    }
    return left.length - right.length;
  }
}
