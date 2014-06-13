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

import c5db.client.generated.Result;
import c5db.client.generated.ScanResponse;

import java.io.IOException;

public interface C5ClientScanner {
  Result next() throws InterruptedException;

  Result[] next(int nbRows) throws IOException, InterruptedException;

  void close() throws InterruptedException;

  void add(ScanResponse response) throws InterruptedException;
}
