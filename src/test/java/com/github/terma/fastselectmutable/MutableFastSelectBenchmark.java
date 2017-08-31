/*
Copyright 2017 Artem Stasiuk

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */
package com.github.terma.fastselectmutable;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Need to get
 * <pre>
 * Throughput: 10m/min
 * Nodes: 8
 * Batch size: 100
 * Throughput per node: 1.25m/min = 21k/sec
 * Batch throughput per node: 0.125m/min = 0.2k/sec
 *
 * Current batch throughput per node: 0.06m/min = 1.1k/sec
 * Current batch throughput: 0.5m/min
 * Current throughput: 50m/min
 * </pre>
 * <p>
 * <pre>
 * Without {@link CommitLog}
 *
 * Benchmark                       (volume)   Mode  Cnt       Score   Error  Units
 * MutableFastSelectBenchmark.add         1  thrpt       194241.319          ops/s
 * MutableFastSelectBenchmark.add        10  thrpt        21649.888          ops/s
 * MutableFastSelectBenchmark.add       100  thrpt         2347.391          ops/s
 *
 * With {@link CommitLog}
 * Benchmark                       (volume)   Mode  Cnt     Score   Error  Units
 * MutableFastSelectBenchmark.add         1  thrpt       4134.951          ops/s
 * MutableFastSelectBenchmark.add        10  thrpt       3154.463          ops/s
 * MutableFastSelectBenchmark.add       100  thrpt       1116.265          ops/s
 *
 * With {@link CommitLog} and realistic data {@link Data100Fields}
 * Benchmark                       (volume)   Mode  Cnt     Score   Error  Units
 * MutableFastSelectBenchmark.add         1  thrpt       3227.405          ops/s
 * MutableFastSelectBenchmark.add       100  thrpt         97.632          ops/s
 *
 * Same plus {@link com.esotericsoftware.kryo.Kryo}
 * Benchmark                       (volume)   Mode  Cnt     Score   Error  Units
 * MutableFastSelectBenchmark.add         1  thrpt       3441.980          ops/s
 * MutableFastSelectBenchmark.add       100  thrpt         93.655          ops/s
 *
 * Same and optimize for bulk write to {@link CommitLog}
 * Benchmark                       (volume)   Mode  Cnt     Score   Error  Units
 * MutableFastSelectBenchmark.add         1  thrpt       3391.210          ops/s
 * MutableFastSelectBenchmark.add       100  thrpt         98.496          ops/s
 *
 * Benchmark                       (volume)   Mode  Cnt     Score   Error  Units
 * MutableFastSelectBenchmark.add         1  thrpt       3532.353          ops/s
 * MutableFastSelectBenchmark.add       100  thrpt         98.409          ops/s
 * MutableFastSelectBenchmark.add         1   avgt         ≈ 10⁻⁴           s/op
 * MutableFastSelectBenchmark.add       100   avgt          0.010           s/op
 *
 * Use {@link Updater}
 * Benchmark                       (volume)   Mode  Cnt     Score   Error  Units
 * MutableFastSelectBenchmark.add         1  thrpt       3523.890          ops/s
 * MutableFastSelectBenchmark.add       100  thrpt        239.273          ops/s
 * MutableFastSelectBenchmark.add      1000  thrpt         24.279          ops/s
 * MutableFastSelectBenchmark.add         1   avgt         ≈ 10⁻⁴           s/op
 * MutableFastSelectBenchmark.add       100   avgt          0.004           s/op
 * MutableFastSelectBenchmark.add      1000   avgt          0.041           s/op
 *
 * 25k/sec = 1.5m/min = 15m/10min
 *
 * With saving of fast-select and commit log flush
 * Benchmark                       (batch)  (commitLogThreshold)   Mode  Cnt     Score   Error  Units
 * MutableFastSelectBenchmark.add        1               1000000  thrpt       2385.185          ops/s
 * MutableFastSelectBenchmark.add        1              10000000  thrpt       2468.029          ops/s
 * MutableFastSelectBenchmark.add      100               1000000  thrpt         60.065          ops/s
 * MutableFastSelectBenchmark.add      100              10000000  thrpt         89.451          ops/s
 * MutableFastSelectBenchmark.add     1000               1000000  thrpt          6.118          ops/s
 * MutableFastSelectBenchmark.add     1000              10000000  thrpt          8.572          ops/s
 * </pre>
 */
@Fork(value = 1, jvmArgs = {"-Xmx2g", "-XX:CompileThreshold=1"})
@BenchmarkMode({Mode.Throughput})
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(time = 15, iterations = 1)
@Measurement(time = 15, iterations = 1)
public class MutableFastSelectBenchmark {

    @Param({"1", "100", "1000"})
    private int batch;

    @Param({"1000000", "10000000"})
    private int commitLogThreshold;

    private Random random = new Random();

    private MutableFastSelect<Data100Fields> mutableFastSelect;
    private List<Data100Fields> batchData = new ArrayList<>();

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder().include("." + MutableFastSelectBenchmark.class.getSimpleName() + ".*").build();
        new Runner(opt).run();
    }

    @Setup
    public void setup() throws IOException {
        final File dir = Files.createTempDirectory("mutable-fast-select-benchmark").toFile();
        dir.deleteOnExit();
        mutableFastSelect = new MutableFastSelect<>(Data100Fields.class, dir, false, commitLogThreshold);

        for (int i = 0; i < batch; i++) {
            Data100Fields data = new Data100Fields();
            data.id = String.valueOf(i);
            data.field1 = "STRING_WITH_DATA" + (i + 1);
            data.field2 = "STRING_WITH_DATA" + (i + 2);
            data.field3 = "STRING_WITH_DATA" + (i + 3);
            data.field4 = "STRING_WITH_DATA" + (i + 4);
            data.field5 = "STRING_WITH_DATA" + (i + 5);
            data.field6 = "STRING_WITH_DATA" + (i + 6);
            data.field7 = "STRING_WITH_DATA" + (i + 7);
            data.field8 = "STRING_WITH_DATA" + (i + 8);
            data.field9 = "STRING_WITH_DATA" + (i + 9);
            data.field10 = "STRING_WITH_DATA" + (i + 10);
            data.field11 = "STRING_WITH_DATA" + (i + 11);
            data.field12 = "STRING_WITH_DATA" + (i + 12);
            data.field13 = "STRING_WITH_DATA" + (i + 13);
            data.field14 = "STRING_WITH_DATA" + (i + 14);
            data.field15 = "STRING_WITH_DATA" + (i + 15);
            data.field16 = "STRING_WITH_DATA" + (i + 16);
            data.field17 = "STRING_WITH_DATA" + (i + 17);
            data.field18 = "STRING_WITH_DATA" + (i + 18);
            data.field19 = "STRING_WITH_DATA" + (i + 19);
            data.field20 = "STRING_WITH_DATA" + (i + 20);
            data.field21 = "STRING_WITH_DATA" + (i + 21);
            data.field22 = "STRING_WITH_DATA" + (i + 22);
            data.field23 = "STRING_WITH_DATA" + (i + 23);
            data.field24 = "STRING_WITH_DATA" + (i + 24);
            data.field25 = "STRING_WITH_DATA" + (i + 25);
            data.field26 = "STRING_WITH_DATA" + (i + 26);
            data.field27 = "STRING_WITH_DATA" + (i + 27);
            data.field28 = "STRING_WITH_DATA" + (i + 28);
            data.field29 = "STRING_WITH_DATA" + (i + 29);
            data.field30 = "STRING_WITH_DATA" + (i + 30);
            data.amount = random.nextLong();
            batchData.add(data);
        }
    }

    @Benchmark
    public Object add() throws Exception {
        mutableFastSelect.modify(new Updater<>(Collections.<String>emptyList(), batchData));
        return mutableFastSelect;
    }

}
