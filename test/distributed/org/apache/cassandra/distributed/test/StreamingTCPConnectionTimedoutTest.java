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

package org.apache.cassandra.distributed.test;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Assert;
import org.junit.Test;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.streaming.CassandraCompressedStreamReader;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.service.NativeTransportService;
import org.apache.cassandra.service.StorageService;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

public class StreamingTCPConnectionTimedoutTest extends TestBaseImpl
{

    public static class BBInterceptor
    {
        public static final AtomicBoolean enabled = new AtomicBoolean(false);

        static void install(ClassLoader cl, int nodeNumber)
        {
            if (nodeNumber != 2)
                return;
            new ByteBuddy().rebase(CassandraCompressedStreamReader.class)
                           .method(named("read").and(takesArguments(1)))
                           .intercept(MethodDelegation.to(BBInterceptor.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        public static SSTableMultiWriter read(DataInputPlus in, @SuperCall Callable<SSTableMultiWriter> r) throws Exception
        {
            if (enabled.get())
            {
                int wait = 290;
                System.out.println("node2 <--- block CassandraCompressedStreamReader.read for " + wait + " seconds");
                Uninterruptibles.sleepUninterruptibly(wait, TimeUnit.SECONDS);
                enabled.set(false);
            }
            return r.call();
        }
    }

    @Test
    public void testTCPConnectionTimedout() throws Throwable
    {
        System.setProperty("io.netty.transport.noNative", "false");

        try (Cluster cluster = builder().withNodes(2)
                                        .withInstanceInitializer(BBInterceptor::install)
                                        .withDataDirCount(1) // this test expects there to only be a single sstable to stream (with ddirs = 3, we get 3 sstables)
                                        .withConfig(config -> config.with(NETWORK, NATIVE_PROTOCOL, GOSSIP)
                                                                    .set("internode_tcp_user_timeout_in_ms", 3000)
                                                                    .set("internode_streaming_tcp_user_timeout_in_ms", 300000)
                                                                    .set("internode_socket_receive_buffer_size_in_bytes", 1024)
                                                                    .set("internode_socket_send_buffer_size_in_bytes", 1024)
                                                                    .set("stream_entire_sstables", false)
                                        ).start())
        {
            cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2 };");
            cluster.schemaChange(String.format("CREATE TABLE %s.cf (k text, c1 text, c2 text, PRIMARY KEY (k)) WITH compaction = {'class': '%s', 'enabled': 'false'}", KEYSPACE, "LeveledCompactionStrategy"));

            for (int n = 1; n <= 2; n++)
            {
                final int num = n;
                cluster.get(n).runOnInstance(() -> {
                    System.out.println("node" + num + " <--- using epoll? " + NativeTransportService.useEpoll());
                    System.out.println("node" + num + " <--- tcp user timeout? " + DatabaseDescriptor.getInternodeStreamingTcpUserTimeoutInMS());
                });
            }
            for (int k = 0; k < 5; k++)
            {
                for (int i = k * 500; i < k * 500 + 500; ++i)
                    cluster.get(1).executeInternal(String.format("INSERT INTO %s.cf (k, c1, c2) VALUES (?, 'value1', 'value2');", KEYSPACE), Integer.toString(i));
                cluster.get(1).nodetool("flush");
            }

            System.out.println("!!!! NUMBER OF SSTABLES " + cluster.get(1).callOnInstance(() -> Keyspace.open("distributed_test_keyspace").getColumnFamilyStore("cf").getLiveSSTables().size()));

            cluster.get(2).executeInternal("TRUNCATE system.available_ranges;");
            {
                Object[][] results = cluster.get(2).executeInternal(String.format("SELECT k, c1, c2 FROM %s.cf;", KEYSPACE));
                Assert.assertEquals(0, results.length);
            }

            cluster.get(2).runOnInstance(() -> {
                BBInterceptor.enabled.set(true);
                StorageService.instance.rebuild(null, KEYSPACE, null, null);
            });
            cluster.checkAndResetUncaughtExceptions();
        }
    }
}
