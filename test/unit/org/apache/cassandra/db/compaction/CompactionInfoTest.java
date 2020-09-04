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

package org.apache.cassandra.db.compaction;

import java.util.ArrayList;
import java.util.UUID;
import java.util.regex.Pattern;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.repair.AbstractPendingAntiCompactionTest;
import org.apache.cassandra.schema.MockSchema;

public class CompactionInfoTest extends AbstractPendingAntiCompactionTest
{
    @Test
    public void testCompactionInfoToStringContainsTaskId()
    {
        ColumnFamilyStore cfs = MockSchema.newCFS();
        UUID expectedTaskId = UUID.randomUUID();
        CompactionInfo compactionInfo = new CompactionInfo(cfs.metadata(), OperationType.COMPACTION, 0, 1000, expectedTaskId, new ArrayList<>());
        String actual = compactionInfo.toString();
        Assert.assertTrue("CompactionInfo string should contain taskId. Actual: " + actual, compactionInfo.toString().contains(expectedTaskId.toString()));
    }

    @Test
    public void testCompactionInfoToStringFormat()
    {
        ColumnFamilyStore cfs = MockSchema.newCFS();
        CompactionInfo compactionInfo = new CompactionInfo(cfs.metadata(), OperationType.COMPACTION, 0, 1000, UUID.randomUUID(), new ArrayList<>());
        String uuidPattern = "([0-9a-fA-F]{8}\\-[0-9a-fA-F]{4}\\-[0-9a-fA-F]{4}\\-[0-9a-fA-F]{4}\\-[0-9a-fA-F]{12})";
        Pattern pattern = Pattern.compile("(\\w+)\\(" + uuidPattern + ", (\\d+) / (\\d+) (\\w+)\\)@" + uuidPattern + "\\((\\w+), (\\w+)\\)");
        String actual = compactionInfo.toString();
        Assert.assertTrue("CompactionInfo string should match pattern. Actual: " + actual +
                          "\nPattern: " + pattern.pattern() +
                          "\nFormat: " + "TaskType(TaskId, Progress Unit)@TableId(KeyspaceName, TableName)",
                          pattern.matcher(compactionInfo.toString()).matches());
    }
}
