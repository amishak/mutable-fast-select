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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

public class CommitLogTest {

    private File dir;

    @Before
    public void prepareFiles() throws IOException {
        dir = Files.createTempDirectory("commit-log").toFile();
        dir.deleteOnExit();
    }

    @Test
    public void create() throws IOException {
        new CommitLog<>(dir, false);
    }

    @Test
    public void addAndLoad() throws IOException {
        Data data1 = new Data();
        data1.id = "1";
        Data data2 = new Data();
        data2.id = "2";

        CommitLog<Data> commitLog = new CommitLog<>(dir, false);
        commitLog.write(new DeleteAndAdd<>(Collections.<Integer>emptyList(), Arrays.asList(data1, data2)));
        Assert.assertEquals(97, commitLog.size());

        Iterable<DeleteAndAdd<Data>> iterable = commitLog.load();

        Iterator<DeleteAndAdd<Data>> iterator = iterable.iterator();
        Assert.assertEquals(true, iterator.hasNext());
        DeleteAndAdd<Data> deleteAndAdd = iterator.next();
        Assert.assertEquals(2, deleteAndAdd.add.size());
        Assert.assertEquals(0, deleteAndAdd.delete.size());
    }

    @Test
    public void addAndLoadFromNewInstance() throws IOException {
        Data data1 = new Data();
        data1.id = "1";
        Data data2 = new Data();
        data2.id = "2";

        CommitLog<Data> commitLog = new CommitLog<>(dir, false);
        commitLog.write(new DeleteAndAdd<>(Collections.<Integer>emptyList(), Arrays.asList(data1, data2)));

        CommitLog<Data> commitLog1 = new CommitLog<>(dir, false);
        Assert.assertEquals(97, commitLog1.size());

        Iterable<DeleteAndAdd<Data>> iterable = commitLog1.load();

        Iterator<DeleteAndAdd<Data>> iterator = iterable.iterator();
        Assert.assertEquals(true, iterator.hasNext());
        DeleteAndAdd<Data> deleteAndAdd = iterator.next();
        Assert.assertEquals(2, deleteAndAdd.add.size());
        Assert.assertEquals(0, deleteAndAdd.delete.size());
    }

    @Test
    public void loadFromEmpty() throws IOException {
        CommitLog<Data> commitLog = new CommitLog<>(dir, false);
        Assert.assertEquals(0, commitLog.size());

        Iterable<DeleteAndAdd<Data>> iterable = commitLog.load();

        Iterator<DeleteAndAdd<Data>> iterator = iterable.iterator();
        Assert.assertEquals(false, iterator.hasNext());
    }

    @Test
    public void clear() throws IOException {
        Data data1 = new Data();
        data1.id = "1";
        Data data2 = new Data();
        data2.id = "2";

        CommitLog<Data> commitLog = new CommitLog<>(dir, false);
        commitLog.write(new DeleteAndAdd<>(Collections.<Integer>emptyList(), Arrays.asList(data1, data2)));
        Assert.assertEquals(97, commitLog.size());
        commitLog.clear();

        Assert.assertEquals(0, commitLog.size());
        Iterable<DeleteAndAdd<Data>> iterable = commitLog.load();
        Iterator<DeleteAndAdd<Data>> iterator = iterable.iterator();
        Assert.assertEquals(false, iterator.hasNext());
    }

    @Test
    public void clearAndWriteMore() throws IOException {
        Data data1 = new Data();
        data1.id = "1";
        Data data2 = new Data();
        data2.id = "2";

        CommitLog<Data> commitLog = new CommitLog<>(dir, false);
        commitLog.write(new DeleteAndAdd<>(Collections.<Integer>emptyList(), Arrays.asList(data1, data2)));
        Assert.assertEquals(97, commitLog.size());
        commitLog.clear();

        commitLog.write(new DeleteAndAdd<>(Collections.<Integer>emptyList(), Arrays.asList(data1, data2)));
        Assert.assertEquals(97, commitLog.size());
        Iterable<DeleteAndAdd<Data>> iterable = commitLog.load();
        Iterator<DeleteAndAdd<Data>> iterator = iterable.iterator();
        Assert.assertEquals(true, iterator.hasNext());
    }

    @Test
    public void clearAndLoadFromNewInstance() throws IOException {
        Data data1 = new Data();
        data1.id = "1";
        Data data2 = new Data();
        data2.id = "2";

        CommitLog<Data> commitLog = new CommitLog<>(dir, false);
        commitLog.write(new DeleteAndAdd<>(Collections.<Integer>emptyList(), Arrays.asList(data1, data2)));
        Assert.assertEquals(97, commitLog.size());
        commitLog.clear();

        CommitLog<Data> commitLog1 = new CommitLog<>(dir, false);
        Assert.assertEquals(0, commitLog1.size());
        Iterable<DeleteAndAdd<Data>> iterable = commitLog1.load();
        Iterator<DeleteAndAdd<Data>> iterator = iterable.iterator();
        Assert.assertEquals(false, iterator.hasNext());
    }

    @SuppressWarnings("WeakerAccess")
    public static class Data implements Item {

        public byte deleted;
        public String id;
        public long amount;

        @Override
        public String getId() {
            return id;
        }
    }

}
