/*
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

package org.apache.cassandra.distributed.api;

import org.apache.cassandra.net.Verb;

public interface IMessageFilters
{
    public interface Filter
    {
        // deactivate filter
        Filter off();
        // activate filter
        Filter on();
        boolean apply();
        // determine if the filter should handle
        boolean matches(int from, int to, int verb);
    }

    public interface Builder
    {
        Builder from(int ... nums);
        Builder to(int ... nums);
        // build a drop filter
        Filter drop();
        // build a filter that invokes the runnable before permitting
        Filter intercept(Runnable runnable);
    }

    Builder verbs(Verb ... verbs);
    Builder allVerbs();
    void reset();

    // internal
    boolean permit(IInstance from, IInstance to, int verb);

}
