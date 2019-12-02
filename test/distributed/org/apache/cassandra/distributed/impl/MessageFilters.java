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

package org.apache.cassandra.distributed.impl;

import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.net.Verb;

public class MessageFilters implements IMessageFilters
{
    private final Set<IMessageFilters.Filter> filters = new CopyOnWriteArraySet<>();

    public boolean permit(IInstance from, IInstance to, int verb)
    {
        if (from == null || to == null)
            return false; // cannot deliver
        int fromNum = from.config().num();
        int toNum = to.config().num();

        for (IMessageFilters.Filter filter : filters)
            if (filter.matches(fromNum, toNum, verb))
                return filter.apply();

        return true;
    }

    protected abstract class FilterBase implements IMessageFilters.Filter
    {
        protected final int[] from;
        protected final int[] to;
        protected final int[] verbs;

        FilterBase(int[] from, int[] to, int[] verbs)
        {
            if (from != null)
            {
                from = from.clone();
                Arrays.sort(from);
            }
            if (to != null)
            {
                to = to.clone();
                Arrays.sort(to);
            }
            if (verbs != null)
            {
                verbs = verbs.clone();
                Arrays.sort(verbs);
            }
            this.from = from;
            this.to = to;
            this.verbs = verbs;
        }

        public boolean matches(int from, int to, int verb)
        {
            return (this.from == null || Arrays.binarySearch(this.from, from) >= 0)
                   && (this.to == null || Arrays.binarySearch(this.to, to) >= 0)
                   && (this.verbs == null || Arrays.binarySearch(this.verbs, verb) >= 0);
        }

        public IMessageFilters.Filter off()
        {
            filters.remove(this);
            return this;
        }

        public IMessageFilters.Filter on()
        {
            filters.add(this);
            return this;
        }

        public int hashCode()
        {
            return (from == null ? 0 : Arrays.hashCode(from))
                   + (to == null ? 0 : Arrays.hashCode(to))
                   + (verbs == null ? 0 : Arrays.hashCode(verbs));
        }

        public boolean equals(Object that)
        {
            return that instanceof FilterBase && equals((FilterBase) that);
        }

        boolean equals(FilterBase that)
        {
            return Arrays.equals(from, that.from)
                   && Arrays.equals(to, that.to)
                   && Arrays.equals(verbs, that.verbs);
        }
    }

    public class DropFilter extends FilterBase
    {
        DropFilter(int[] from, int[] to, int[] verbs)
        {
            super(from, to, verbs);
        }

        public boolean apply()
        {
            return false;
        }
    }

    public class InterceptFilter extends FilterBase
    {
        final Runnable runnable;

        InterceptFilter(int[] from, int[] to, int[] verbs, Runnable runnable)
        {
            super(from, to, verbs);
            this.runnable = runnable;
        }

        public boolean apply()
        {
            runnable.run();
            return true;
        }
    }

    public class Builder implements IMessageFilters.Builder
    {
        int[] from;
        int[] to;
        int[] verbs;

        private Builder(int[] verbs)
        {
            this.verbs = verbs;
        }

        public Builder from(int ... nums)
        {
            from = nums;
            return this;
        }

        public Builder to(int ... nums)
        {
            to = nums;
            return this;
        }

        public DropFilter drop()
        {
            return new DropFilter(from, to, verbs);
        }

        public InterceptFilter intercept(Runnable runnable)
        {
            return new InterceptFilter(from, to, verbs, runnable);
        }
    }

    public Builder verbs(Verb... verbs)
    {
        int[] ids = new int[verbs.length];
        for (int i = 0 ; i < verbs.length ; ++i)
            ids[i] = verbs[i].id;
        return new Builder(ids);
    }

    @Override
    public Builder allVerbs()
    {
        return new Builder(null);
    }

    @Override
    public void reset()
    {
        filters.clear();
    }

}
