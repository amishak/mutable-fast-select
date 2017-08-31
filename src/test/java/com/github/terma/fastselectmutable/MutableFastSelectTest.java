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

import com.github.terma.fastselect.FastSelect;
import com.github.terma.fastselect.data.ByteData;
import com.github.terma.fastselect.data.LongData;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;

public class MutableFastSelectTest {

    private File dir;

    @Before
    public void prepareFiles() throws IOException {
        dir = Files.createTempDirectory("mutable-fast-select").toFile();
        dir.deleteOnExit();
    }

    @Test
    public void create() throws IOException {
        new MutableFastSelect<>(Data.class, dir, false);
    }

    @Test
    public void add() throws IOException {
        Data data1 = new Data();
        data1.id = "1";
        Data data2 = new Data();
        data2.id = "2";

        MutableFastSelect<Data> mutableFastSelect = new MutableFastSelect<>(Data.class, dir, false);
        mutableFastSelect.modify(Updater.update(data1, data2));

        CatchSelector catchSelector = new CatchSelector();
        mutableFastSelect.select(catchSelector);
        Assert.assertEquals(2, catchSelector.data.size());
        Assert.assertEquals(2, catchSelector.positions.size());
    }

    @Test
    public void delete() throws IOException {
        Data data1 = new Data();
        data1.id = "1";
        Data data2 = new Data();
        data2.id = "2";

        MutableFastSelect<Data> mutableFastSelect = new MutableFastSelect<>(Data.class, dir, false);
        mutableFastSelect.modify(Updater.update(data1, data2));
        mutableFastSelect.modify(Updater.<Data>delete("1", "2"));

        CatchSelector catchSelector = new CatchSelector();
        mutableFastSelect.select(catchSelector);
        ByteData deleted = (ByteData) catchSelector.data.getColumnsByNames().get("deleted").data;
        Assert.assertEquals(2, catchSelector.data.size());
        Assert.assertEquals((byte) 1, deleted.get(0));
        Assert.assertEquals((byte) 1, deleted.get(1));
        Assert.assertEquals(0, catchSelector.positions.size());
    }

    @Test
    public void update() throws IOException {
        Data data1 = new Data();
        data1.id = "1";
        Data data2 = new Data();
        data2.id = "2";

        MutableFastSelect<Data> mutableFastSelect = new MutableFastSelect<>(Data.class, dir, false);
        mutableFastSelect.modify(Updater.update(data1, data2));

        data1.amount = 12;
        data2.amount = 13;
        mutableFastSelect.modify(Updater.update(data1, data2));

        CatchSelector catchSelector = new CatchSelector();
        mutableFastSelect.select(catchSelector);
        ByteData deleted = (ByteData) catchSelector.data.getColumnsByNames().get("deleted").data;
        LongData amountData = (LongData) catchSelector.data.getColumnsByNames().get("amount").data;
        Assert.assertEquals(4, catchSelector.data.size());
        Assert.assertEquals((byte) 1, deleted.get(0));
        Assert.assertEquals((byte) 1, deleted.get(1));
        Assert.assertEquals((long) 12, amountData.get(2));
        Assert.assertEquals((long) 13, amountData.get(3));
        Assert.assertEquals(2, catchSelector.positions.size());
    }

    @Test
    public void shouldFlushWhenReachCommitLogThreshold() throws IOException {
        Data data1 = new Data();
        data1.id = "1";
        Data data2 = new Data();
        data2.id = "2";

        MutableFastSelect<Data> mutableFastSelect = new MutableFastSelect<>(Data.class, dir, false, 0);
        mutableFastSelect.modify(Updater.update(data1, data2));

//        CatchSelector catchSelector = new CatchSelector();
//        mutableFastSelect.select(catchSelector);
//        ByteData deleted = (ByteData) catchSelector.data.getColumnsByNames().get("deleted").data;
//        LongData amountData = (LongData) catchSelector.data.getColumnsByNames().get("amount").data;
//        Assert.assertEquals(4, catchSelector.data.size());
//        Assert.assertEquals((byte) 1, deleted.get(0));
//        Assert.assertEquals((byte) 1, deleted.get(1));
//        Assert.assertEquals((long) 12, amountData.get(2));
//        Assert.assertEquals((long) 13, amountData.get(3));
//        Assert.assertEquals(2, catchSelector.positions.size());
    }

    @SuppressWarnings("WeakerAccess")
    private static class CatchSelector implements Selector<Data> {

        public FastSelect<Data> data;
        public Map<Object, List<Integer>> positions;

        @Override
        public void execute(FastSelect<Data> data, Map<Object, List<Integer>> positions) {
            this.data = data;
            this.positions = positions;
        }
    }

    @SuppressWarnings("WeakerAccess")
    public static class Data implements Item {

        public byte deleted;
        public String id;
        public long amount;

        @Override
        public Object getId() {
            return id;
        }
    }

}
