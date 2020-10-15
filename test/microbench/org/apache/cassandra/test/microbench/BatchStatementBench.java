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

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.Attributes;
import org.apache.cassandra.cql3.BatchQueryOptions;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.VariableSpecifications;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.profile.GCProfiler;
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

        DatabaseDescriptor.toolInitialization();
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
        KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(keyspace);
        TableMetadata metadata = CreateTableStatement.parse(String.format("CREATE TABLE %s (id int, ck int, v int, primary key (id, ck))", table), keyspace).build();

        Schema.instance.load(ksm.withSwapped(ksm.tables.with(metadata)));

        List<ModificationStatement> modifications = new ArrayList<>(batchSize);
        List<List<ByteBuffer>> parameters = new ArrayList<>(batchSize);
        List<Object> queryOrIdList = new ArrayList<>(batchSize);
        QueryHandler.Prepared prepared = QueryProcessor.prepareInternal(String.format("INSERT INTO %s.%s (id, ck, v) VALUES (?,?,?)", keyspace, table));

        for (int i = 0; i < batchSize; i++)
        {
            modifications.add((ModificationStatement) prepared.statement);
            parameters.add(Lists.newArrayList(bytes(uniquePartition ? i : 1), bytes(i), bytes(i)));
            queryOrIdList.add(prepared.rawCQLStatement);
        }
        bs = new BatchStatement(BatchStatement.Type.UNLOGGED, VariableSpecifications.empty(), modifications, Attributes.none());
        bqo = BatchQueryOptions.withPerStatementVariables(QueryOptions.DEFAULT, parameters, queryOrIdList);
    }

    @Benchmark
    public void bench()
    {
        bs.getMutations(bqo, false, nowInSec, nowInSec, queryStartTime);
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

BASELINE:
Benchmark                                                   (batchSize)  (uniquePartition)   Mode  Cnt          Score        Error   Units
BatchStatementBench.bench                                         10000               true  thrpt    5          0.002 ±      0.001  ops/ms
BatchStatementBench.bench                                         10000              false  thrpt    5          0.282 ±      0.060  ops/ms
BatchStatementBench.bench:·gc.alloc.rate                          10000               true  thrpt    5        899.921 ±     82.375  MB/sec
BatchStatementBench.bench:·gc.alloc.rate                          10000              false  thrpt    5       2821.323 ±    595.978  MB/sec
BatchStatementBench.bench:·gc.alloc.rate.norm                     10000               true  thrpt    5  415412801.379 ±   6266.050    B/op
BatchStatementBench.bench:·gc.alloc.rate.norm                     10000              false  thrpt    5   11001374.497 ±   4705.964    B/op
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

First improvement (correct sizing of btrees)

Benchmark                                                   (batchSize)  (uniquePartition)   Mode  Cnt         Score        Error   Units
BatchStatementBench.bench                                         10000               true  thrpt    5         0.156 ±      0.019  ops/ms
BatchStatementBench.bench                                         10000              false  thrpt    5         0.242 ±      0.020  ops/ms
BatchStatementBench.bench:·gc.alloc.rate                          10000               true  thrpt    5      2270.651 ±    277.751  MB/sec
BatchStatementBench.bench:·gc.alloc.rate                          10000              false  thrpt    5      2460.907 ±    199.001  MB/sec
BatchStatementBench.bench:·gc.alloc.rate.norm                     10000               true  thrpt    5  15983914.072 ±     78.819    B/op
BatchStatementBench.bench:·gc.alloc.rate.norm                     10000              false  thrpt    5  11199544.454 ±   4893.557    B/op
BatchStatementBench.bench:·gc.churn.PS_Eden_Space                 10000               true  thrpt    5      2273.122 ±    246.812  MB/sec
BatchStatementBench.bench:·gc.churn.PS_Eden_Space                 10000              false  thrpt    5      2460.458 ±    205.390  MB/sec
BatchStatementBench.bench:·gc.churn.PS_Eden_Space.norm            10000               true  thrpt    5  16002764.900 ± 223813.282    B/op
BatchStatementBench.bench:·gc.churn.PS_Eden_Space.norm            10000              false  thrpt    5  11197409.259 ± 136683.073    B/op
BatchStatementBench.bench:·gc.churn.PS_Survivor_Space             10000               true  thrpt    5        16.124 ±      3.059  MB/sec
BatchStatementBench.bench:·gc.churn.PS_Survivor_Space             10000              false  thrpt    5         7.509 ±      0.816  MB/sec
BatchStatementBench.bench:·gc.churn.PS_Survivor_Space.norm        10000               true  thrpt    5    113473.209 ±  12716.891    B/op
BatchStatementBench.bench:·gc.churn.PS_Survivor_Space.norm        10000              false  thrpt    5     34171.767 ±   2180.417    B/op
BatchStatementBench.bench:·gc.count                               10000               true  thrpt    5       796.000               counts
BatchStatementBench.bench:·gc.count                               10000              false  thrpt    5       786.000               counts
BatchStatementBench.bench:·gc.time                                10000               true  thrpt    5      1277.000                   ms
BatchStatementBench.bench:·gc.time                                10000              false  thrpt    5       936.000                   ms


Second improvement (avoid creating list + sublist in BTree)

Benchmark                                                   (batchSize)  (uniquePartition)   Mode  Cnt         Score        Error   Units
BatchStatementBench.bench                                         10000               true  thrpt    5         0.162 ±      0.024  ops/ms
BatchStatementBench.bench                                         10000              false  thrpt    5         0.258 ±      0.010  ops/ms
BatchStatementBench.bench:·gc.alloc.rate                          10000               true  thrpt    5      2225.333 ±    324.801  MB/sec
BatchStatementBench.bench:·gc.alloc.rate                          10000              false  thrpt    5      2422.198 ±     96.492  MB/sec
BatchStatementBench.bench:·gc.alloc.rate.norm                     10000               true  thrpt    5  15103912.227 ±     78.683    B/op
BatchStatementBench.bench:·gc.alloc.rate.norm                     10000              false  thrpt    5  10322433.112 ±  20451.722    B/op
BatchStatementBench.bench:·gc.churn.PS_Eden_Space                 10000               true  thrpt    5      2228.625 ±    332.763  MB/sec
BatchStatementBench.bench:·gc.churn.PS_Eden_Space                 10000              false  thrpt    5      2423.556 ±     79.432  MB/sec
BatchStatementBench.bench:·gc.churn.PS_Eden_Space.norm            10000               true  thrpt    5  15125921.280 ± 209690.849    B/op
BatchStatementBench.bench:·gc.churn.PS_Eden_Space.norm            10000              false  thrpt    5  10328414.649 ± 141075.139    B/op
BatchStatementBench.bench:·gc.churn.PS_Survivor_Space             10000               true  thrpt    5        28.191 ±      4.427  MB/sec
BatchStatementBench.bench:·gc.churn.PS_Survivor_Space             10000              false  thrpt    5         6.371 ±      0.170  MB/sec
BatchStatementBench.bench:·gc.churn.PS_Survivor_Space.norm        10000               true  thrpt    5    191323.642 ±   3656.406    B/op
BatchStatementBench.bench:·gc.churn.PS_Survivor_Space.norm        10000              false  thrpt    5     27150.543 ±   1065.905    B/op
BatchStatementBench.bench:·gc.count                               10000               true  thrpt    5       774.000               counts
BatchStatementBench.bench:·gc.count                               10000              false  thrpt    5       774.000               counts
BatchStatementBench.bench:·gc.time                                10000               true  thrpt    5      1239.000                   ms
BatchStatementBench.bench:·gc.time                                10000              false  thrpt    5       928.000                   ms

Third improvement (Avoid creating operation iterator in ModificationStatement)

Benchmark                                                   (batchSize)  (uniquePartition)   Mode  Cnt         Score        Error   Units
BatchStatementBench.bench                                         10000               true  thrpt    5         0.182 ±      0.030  ops/ms
BatchStatementBench.bench                                         10000              false  thrpt    5         0.261 ±      0.021  ops/ms
BatchStatementBench.bench:·gc.alloc.rate                          10000               true  thrpt    5      2248.482 ±    372.207  MB/sec
BatchStatementBench.bench:·gc.alloc.rate                          10000              false  thrpt    5      2085.016 ±    166.204  MB/sec
BatchStatementBench.bench:·gc.alloc.rate.norm                     10000               true  thrpt    5  13583909.108 ±     83.260    B/op
BatchStatementBench.bench:·gc.alloc.rate.norm                     10000              false  thrpt    5   8802104.851 ±  16525.724    B/op
BatchStatementBench.bench:·gc.churn.PS_Eden_Space                 10000               true  thrpt    5      2247.382 ±    372.869  MB/sec
BatchStatementBench.bench:·gc.churn.PS_Eden_Space                 10000              false  thrpt    5      2083.453 ±    176.328  MB/sec
BatchStatementBench.bench:·gc.churn.PS_Eden_Space.norm            10000               true  thrpt    5  13577273.089 ± 150318.557    B/op
BatchStatementBench.bench:·gc.churn.PS_Eden_Space.norm            10000              false  thrpt    5   8795361.471 ± 130992.977    B/op
BatchStatementBench.bench:·gc.churn.PS_Survivor_Space             10000               true  thrpt    5        23.106 ±      4.103  MB/sec
BatchStatementBench.bench:·gc.churn.PS_Survivor_Space             10000              false  thrpt    5         6.657 ±      0.325  MB/sec
BatchStatementBench.bench:·gc.churn.PS_Survivor_Space.norm        10000               true  thrpt    5    139587.040 ±   7490.667    B/op
BatchStatementBench.bench:·gc.churn.PS_Survivor_Space.norm        10000              false  thrpt    5     28108.033 ±   1213.773    B/op
BatchStatementBench.bench:·gc.count                               10000               true  thrpt    5       778.000               counts
BatchStatementBench.bench:·gc.count                               10000              false  thrpt    5       666.000               counts
BatchStatementBench.bench:·gc.time                                10000               true  thrpt    5      1255.000                   ms
BatchStatementBench.bench:·gc.time                                10000              false  thrpt    5       770.000                   ms

Fourth (avoid calling System.currentTimeMillis for every mutation)
Benchmark                                                   (batchSize)  (uniquePartition)   Mode  Cnt         Score        Error   Units
BatchStatementBench.bench                                         10000               true  thrpt    5         0.188 ±      0.015  ops/ms
BatchStatementBench.bench                                         10000              false  thrpt    5         0.276 ±      0.028  ops/ms
BatchStatementBench.bench:·gc.alloc.rate                          10000               true  thrpt    5      2321.796 ±    178.694  MB/sec
BatchStatementBench.bench:·gc.alloc.rate                          10000              false  thrpt    5      2207.746 ±    224.600  MB/sec
BatchStatementBench.bench:·gc.alloc.rate.norm                     10000               true  thrpt    5  13583916.098 ±     79.908    B/op
BatchStatementBench.bench:·gc.alloc.rate.norm                     10000              false  thrpt    5   8800909.373 ±  16611.773    B/op
BatchStatementBench.bench:·gc.churn.PS_Eden_Space                 10000               true  thrpt    5      2319.884 ±    195.784  MB/sec
BatchStatementBench.bench:·gc.churn.PS_Eden_Space                 10000              false  thrpt    5      2209.864 ±    230.349  MB/sec
BatchStatementBench.bench:·gc.churn.PS_Eden_Space.norm            10000               true  thrpt    5  13572326.068 ± 142316.173    B/op
BatchStatementBench.bench:·gc.churn.PS_Eden_Space.norm            10000              false  thrpt    5   8809239.504 ±  55647.480    B/op
BatchStatementBench.bench:·gc.churn.PS_Survivor_Space             10000               true  thrpt    5        23.822 ±      3.427  MB/sec
BatchStatementBench.bench:·gc.churn.PS_Survivor_Space             10000              false  thrpt    5         7.012 ±      0.941  MB/sec
BatchStatementBench.bench:·gc.churn.PS_Survivor_Space.norm        10000               true  thrpt    5    139345.279 ±  11385.439    B/op
BatchStatementBench.bench:·gc.churn.PS_Survivor_Space.norm        10000              false  thrpt    5     27948.527 ±   1227.768    B/op
BatchStatementBench.bench:·gc.count                               10000               true  thrpt    5       803.000               counts
BatchStatementBench.bench:·gc.count                               10000              false  thrpt    5       706.000               counts
BatchStatementBench.bench:·gc.time                                10000               true  thrpt    5      1274.000                   ms
BatchStatementBench.bench:·gc.time                                10000              false  thrpt    5       841.000                   ms


Fifth (track per-table and more list iterator removals):
Benchmark                                                   (batchSize)  (uniquePartition)   Mode  Cnt         Score        Error   Units
BatchStatementBench.bench                                         10000               true  thrpt    5         0.188 ±      0.011  ops/ms
BatchStatementBench.bench:·gc.alloc.rate                          10000               true  thrpt    5      2323.920 ±    138.326  MB/sec
BatchStatementBench.bench:·gc.alloc.rate.norm                     10000               true  thrpt    5  13584027.806 ±     80.888    B/op
BatchStatementBench.bench:·gc.churn.PS_Eden_Space                 10000               true  thrpt    5      2326.652 ±    134.479  MB/sec
BatchStatementBench.bench:·gc.churn.PS_Eden_Space.norm            10000               true  thrpt    5  13600086.950 ±  91090.088    B/op
BatchStatementBench.bench:·gc.churn.PS_Survivor_Space             10000               true  thrpt    5        24.409 ±      2.562  MB/sec
BatchStatementBench.bench:·gc.churn.PS_Survivor_Space.norm        10000               true  thrpt    5    142692.220 ±  15037.251    B/op
BatchStatementBench.bench:·gc.count                               10000               true  thrpt    5       806.000               counts
BatchStatementBench.bench:·gc.time                                10000               true  thrpt    5      1292.000                   ms
BatchStatementBench.bench                                         10000              false  thrpt    5         0.278 ±      0.002  ops/ms
BatchStatementBench.bench:·gc.alloc.rate                          10000              false  thrpt    5      2222.597 ±     22.103  MB/sec
BatchStatementBench.bench:·gc.alloc.rate.norm                     10000              false  thrpt    5   8801313.783 ±  16482.430    B/op
BatchStatementBench.bench:·gc.churn.PS_Eden_Space                 10000              false  thrpt    5      2222.524 ±     38.311  MB/sec
BatchStatementBench.bench:·gc.churn.PS_Eden_Space.norm            10000              false  thrpt    5   8801021.086 ± 123070.457    B/op
BatchStatementBench.bench:·gc.churn.PS_Survivor_Space             10000              false  thrpt    5         7.125 ±      0.177  MB/sec
BatchStatementBench.bench:·gc.churn.PS_Survivor_Space.norm        10000              false  thrpt    5     28215.783 ±    796.481    B/op
BatchStatementBench.bench:·gc.count                               10000              false  thrpt    5       710.000               counts
BatchStatementBench.bench:·gc.time                                10000              false  thrpt    5       858.000                   ms


trunk:
Benchmark                                                   (batchSize)  (uniquePartition)   Mode  Cnt          Score          Error   Units
BatchStatementBench.bench                                         10000               true  thrpt    5          0.002 ±        0.001  ops/ms
BatchStatementBench.bench:·gc.alloc.rate                          10000               true  thrpt    5        826.496 ±      305.322  MB/sec
BatchStatementBench.bench:·gc.alloc.rate.norm                     10000               true  thrpt    5  416702343.006 ±    11120.091    B/op
BatchStatementBench.bench:·gc.churn.PS_Eden_Space                 10000               true  thrpt    5        834.101 ±      305.938  MB/sec
BatchStatementBench.bench:·gc.churn.PS_Eden_Space.norm            10000               true  thrpt    5  420558947.038 ±  6213128.013    B/op
BatchStatementBench.bench:·gc.churn.PS_Old_Gen                    10000               true  thrpt    5        655.051 ±      225.119  MB/sec
BatchStatementBench.bench:·gc.churn.PS_Old_Gen.norm               10000               true  thrpt    5  330517639.785 ± 32161705.341    B/op
BatchStatementBench.bench:·gc.churn.PS_Survivor_Space             10000               true  thrpt    5        118.265 ±       42.681  MB/sec
BatchStatementBench.bench:·gc.churn.PS_Survivor_Space.norm        10000               true  thrpt    5   59639844.759 ±  2770710.412    B/op
BatchStatementBench.bench:·gc.count                               10000               true  thrpt    5       1051.000                 counts
BatchStatementBench.bench:·gc.time                                10000               true  thrpt    5      47590.000                     ms
BatchStatementBench.bench                                         10000              false  thrpt    5          0.276 ±        0.012  ops/ms
BatchStatementBench.bench:·gc.alloc.rate                          10000              false  thrpt    5       2111.582 ±       89.435  MB/sec
BatchStatementBench.bench:·gc.alloc.rate.norm                     10000              false  thrpt    5    8411473.517 ±     2963.134    B/op
BatchStatementBench.bench:·gc.churn.PS_Eden_Space                 10000              false  thrpt    5       2110.659 ±       85.340  MB/sec
BatchStatementBench.bench:·gc.churn.PS_Eden_Space.norm            10000              false  thrpt    5    8407851.183 ±    78180.122    B/op
BatchStatementBench.bench:·gc.churn.PS_Survivor_Space             10000              false  thrpt    5          4.909 ±        0.172  MB/sec
BatchStatementBench.bench:·gc.churn.PS_Survivor_Space.norm        10000              false  thrpt    5      19556.836 ±      328.343    B/op
BatchStatementBench.bench:·gc.count                               10000              false  thrpt    5        670.000                 counts
BatchStatementBench.bench:·gc.time                                10000              false  thrpt    5        692.000                     ms
API replied benchmark score: 0.002182122682368609 ops/ms over 5 iterations
API replied benchmark score: 0.27646888889584986 ops/ms over 5 iterations


     */
}
