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

package org.apache.cassandra.test.microbench;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;

import org.apache.cassandra.UpdateBuilder;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.Attributes;
import org.apache.cassandra.cql3.BatchQueryOptions;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBufferFixed;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.utils.FBUtilities;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.profile.HotspotMemoryProfiler;
import org.openjdk.jmh.profile.StackProfiler;
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;


@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1,jvmArgsAppend = "-Xmx512M")
@Threads(1)
@State(Scope.Benchmark)
public class BatchStatementBench
{
    static
    {
        DatabaseDescriptor.clientInitialization();
        // Partitioner is not set in client mode.
        if (DatabaseDescriptor.getPartitioner() == null)
            DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    static String keyspace = "keyspace1";
    String table = "tbl";

    int nowInSec = FBUtilities.nowInSeconds();
    long queryStartTime = System.nanoTime();
    BatchStatement bs;
    BatchQueryOptions bqo;

    @Param({"true", "false"})
    boolean uniquePartition;

    @Param({"10000"})
    int batchSize;

    @Setup
    public void setup() throws Throwable
    {
        Schema.instance.load(KeyspaceMetadata.create(keyspace, KeyspaceParams.simple(1)));
        KeyspaceMetadata ksm = Schema.instance.getKSMetaData(keyspace);
        CFMetaData metadata = CFMetaData.compile(String.format("CREATE TABLE %s (id int, ck int, v int, primary key (id, ck))", table), keyspace);

        Schema.instance.load(metadata);
        Schema.instance.setKeyspaceMetadata(ksm.withSwapped(ksm.tables.with(metadata)));

        List<ModificationStatement> modifications = new ArrayList<>(batchSize);
        List<List<ByteBuffer>> parameters = new ArrayList<>(batchSize);
        List<Object> queryOrIdList = new ArrayList<>(batchSize);
        ParsedStatement.Prepared prepared = QueryProcessor.parseStatement(String.format("INSERT INTO %s.%s (id, ck, v) VALUES (?,?,?)", keyspace, table), QueryState.forInternalCalls());

        for (int i = 0; i < batchSize; i++)
        {
            modifications.add((ModificationStatement) prepared.statement);
            parameters.add(Lists.newArrayList(bytes(uniquePartition ? i : 1), bytes(i), bytes(i)));
            queryOrIdList.add(prepared.rawCQLStatement);
        }
        bs = new BatchStatement(3, BatchStatement.Type.UNLOGGED, modifications, Attributes.none());
        bqo = BatchQueryOptions.withPerStatementVariables(QueryOptions.DEFAULT, parameters, queryOrIdList);
    }

    @Benchmark
    public void bench()
    {
        bs.getMutations(bqo, false, nowInSec, queryStartTime);
    }


    public static void main(String... args) throws Exception {
        Options opts = new OptionsBuilder()
                       .include(".*"+BatchStatementBench.class.getSimpleName()+".*")
                       .jvmArgs("-server")
                       .forks(1)
                       .mode(Mode.Throughput)
                       .addProfiler(GCProfiler.class)
                       .build();

        Collection<RunResult> records = new Runner(opts).run();
        for ( RunResult result : records) {
            Result r = result.getPrimaryResult();
            System.out.println("API replied benchmark score: "
                               + r.getScore() + " "
                               + r.getScoreUnit() + " over "
                               + r.getStatistics().getN() + " iterations");
        }
    }


    /*
Benchmark                                                   (batchSize)  (uniquePartition)   Mode  Cnt          Score        Error   Units
BatchStatementBench.bench                                         10000               true  thrpt    5          0.002 ±      0.001  ops/ms
BatchStatementBench.bench                                         10000              false  thrpt    5          0.282 ±      0.060  ops/ms
BatchStatementBench.bench:·gc.alloc.rate                          10000               true  thrpt    5        899.921 ±     82.375  MB/sec
BatchStatementBench.bench:·gc.alloc.rate                          10000              false  thrpt    5       2821.323 ±    595.978  MB/sec
BatchStatementBench.bench:·gc.alloc.rate.norm                     10000              false  thrpt    5   11001374.497 ±   4705.964    B/op
BatchStatementBench.bench:·gc.alloc.rate.norm                     10000               true  thrpt    5  415412801.379 ±   6266.050    B/op
BatchStatementBench.bench:·gc.churn.PS_Eden_Space                 10000               true  thrpt    5        910.225 ±     83.500  MB/sec
BatchStatementBench.bench:·gc.churn.PS_Eden_Space                 10000              false  thrpt    5       2820.142 ±    597.190  MB/sec
BatchStatementBench.bench:·gc.churn.PS_Eden_Space.norm            10000               true  thrpt    5  420169006.389 ± 368132.246    B/op
BatchStatementBench.bench:·gc.churn.PS_Eden_Space.norm            10000              false  thrpt    5   10996718.493 ±  63323.428    B/op
BatchStatementBench.bench:·gc.churn.PS_Survivor_Space             10000               true  thrpt    5        127.985 ±     11.445  MB/sec
BatchStatementBench.bench:·gc.churn.PS_Survivor_Space             10000              false  thrpt    5          4.943 ±      0.990  MB/sec
BatchStatementBench.bench:·gc.churn.PS_Survivor_Space.norm        10000               true  thrpt    5   59079633.611 ± 142786.571    B/op
BatchStatementBench.bench:·gc.churn.PS_Survivor_Space.norm        10000              false  thrpt    5      19279.688 ±    726.436    B/op
BatchStatementBench.bench:·gc.count                               10000               true  thrpt    5       1098.000               counts
BatchStatementBench.bench:·gc.count                               10000              false  thrpt    5        895.000               counts
BatchStatementBench.bench:·gc.time                                10000               true  thrpt    5      47690.000                   ms
BatchStatementBench.bench:·gc.time                                10000              false  thrpt    5       1030.000                   ms












     */
}
