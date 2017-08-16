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

import com.github.terma.fastselect.ByteRequest;
import com.github.terma.fastselect.FastSelect;
import com.github.terma.fastselect.Request;
import com.github.terma.fastselect.callbacks.ArrayLayoutCallback;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark                                      (batch)  (initial)   Mode  Cnt    Score   Error  Units
 * MutableFastSelectConcurrentBenchmark.a               1     500000  thrpt       876.887          ops/s
 * MutableFastSelectConcurrentBenchmark.a:add           1     500000  thrpt       853.962          ops/s
 * MutableFastSelectConcurrentBenchmark.a:select        1     500000  thrpt        22.925          ops/s
 * MutableFastSelectConcurrentBenchmark.a             100     500000  thrpt        29.943          ops/s
 * MutableFastSelectConcurrentBenchmark.a:add         100     500000  thrpt        24.282          ops/s
 * MutableFastSelectConcurrentBenchmark.a:select      100     500000  thrpt         5.661          ops/s
 * MutableFastSelectConcurrentBenchmark.a            1000     500000  thrpt         3.271          ops/s
 * MutableFastSelectConcurrentBenchmark.a:add        1000     500000  thrpt         2.877          ops/s
 * MutableFastSelectConcurrentBenchmark.a:select     1000     500000  thrpt         0.394          ops/s
 * <p>
 * MutableFastSelectConcurrentBenchmark.mixed                    1     500000  thrpt       1111.234          ops/s
 * MutableFastSelectConcurrentBenchmark.mixed:mixedAdd           1     500000  thrpt       1073.444          ops/s
 * MutableFastSelectConcurrentBenchmark.mixed:mixedSelect        1     500000  thrpt         37.791          ops/s
 * MutableFastSelectConcurrentBenchmark.mixed                  100     500000  thrpt         64.126          ops/s
 * MutableFastSelectConcurrentBenchmark.mixed:mixedAdd         100     500000  thrpt         61.993          ops/s
 * MutableFastSelectConcurrentBenchmark.mixed:mixedSelect      100     500000  thrpt          2.133          ops/s
 * MutableFastSelectConcurrentBenchmark.mixed                 1000     500000  thrpt          7.556          ops/s
 * MutableFastSelectConcurrentBenchmark.mixed:mixedAdd        1000     500000  thrpt          5.088          ops/s
 * MutableFastSelectConcurrentBenchmark.mixed:mixedSelect     1000     500000  thrpt          2.469          ops/s
 * MutableFastSelectConcurrentBenchmark.readonly                 1     500000  thrpt        522.996          ops/s
 * MutableFastSelectConcurrentBenchmark.readonly               100     500000  thrpt        750.873          ops/s
 * MutableFastSelectConcurrentBenchmark.readonly              1000     500000  thrpt        772.254          ops/s
 */
@Fork(value = 1, jvmArgs = {"-Xmx2g", "-XX:CompileThreshold=1"})
@BenchmarkMode({Mode.Throughput})
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Group)
@Warmup(time = 15, iterations = 1)
@Measurement(time = 15, iterations = 1)
public class MutableFastSelectConcurrentBenchmark {

    @Param({"1", "100", "1000"})
    private int batch;

    @Param("500000")
    private int initial;

    private Random random = new Random();

    private MutableFastSelect<Data100Fields> mutableFastSelect;
    private List<Data100Fields> batchData = new ArrayList<>();

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder().include("." + MutableFastSelectConcurrentBenchmark.class.getSimpleName() + ".*").build();
        new Runner(opt).run();
    }

    @Setup
    public void setup() throws IOException {
        final File dir = Files.createTempDirectory("mutable-fast-select-benchmark").toFile();
        dir.deleteOnExit();
        mutableFastSelect = new MutableFastSelect<>(Data100Fields.class, dir, false);

        List<Data100Fields> init = new ArrayList<>();
        for (int i = 0; i < initial; i++) {
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
            init.add(data);
            if (init.size() > 10000) {
                mutableFastSelect.update(new Updater<>(Collections.<String>emptyList(), init));
                init.clear();
            }
        }
        if (init.size() > 0) {
            mutableFastSelect.update(new Updater<>(Collections.<String>emptyList(), init));
            init.clear();
        }


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

    @GroupThreads
    @Group("mixed")
    @Benchmark
    public Object mixedAdd() throws Exception {
        mutableFastSelect.update(new Updater<>(Collections.<String>emptyList(), batchData));
        return mutableFastSelect;
    }

    @GroupThreads(2)
    @Group("mixed")
    @Benchmark
    public void mixedSelect(final Blackhole blackhole) throws Exception {
        mutableFastSelect.select(new Selector<Data100Fields>() {
            @Override
            public void execute(FastSelect<Data100Fields> data, Map<String, List<Integer>> positions) {
                data.select(new Request[]{new ByteRequest("deleted", 0)}, new ArrayLayoutCallback() {
                    @Override
                    public void data(int position) {
                        blackhole.l2 = position;
                    }
                });
            }
        });
    }

    @GroupThreads(3)
    @Group("readonly")
    @Benchmark
    public void readonlySelect(final Blackhole blackhole) throws Exception {
        mutableFastSelect.select(new Selector<Data100Fields>() {
            @Override
            public void execute(FastSelect<Data100Fields> data, Map<String, List<Integer>> positions) {
                data.select(new Request[]{new ByteRequest("deleted", 0)}, new ArrayLayoutCallback() {
                    @Override
                    public void data(int position) {
                        blackhole.l2 = position;
                    }
                });
            }
        });
    }

}
