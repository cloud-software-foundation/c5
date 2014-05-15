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

/** Incorporates changes licensed under:
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package c5db.client;


import c5db.client.generated.Get;
import c5db.client.generated.GetRequest;
import c5db.client.generated.MultiRequest;
import c5db.client.generated.MutateRequest;
import c5db.client.generated.Response;
import c5db.client.generated.Result;
import c5db.client.generated.Scan;
import c5db.client.generated.ScanRequest;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

/**
 * A shared interface so that a user can interchange HTable and C5Table.
 */
public interface TableInterface extends AutoCloseable {

  ListenableFuture<Response> dotGetCall(GetRequest get);

  ListenableFuture<Response>  doScanCall(ScanRequest scanRequest);

  ListenableFuture<Response>  doMutateCall(MutateRequest mutateRequest);

  ListenableFuture<Response> doMultiCall(MultiRequest multiRequest);
}
