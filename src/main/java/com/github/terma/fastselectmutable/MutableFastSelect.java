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
import com.github.terma.fastselect.FastSelectBuilder;
import com.github.terma.fastselect.data.ByteData;
import com.github.terma.fastselect.data.StringData;

import javax.annotation.concurrent.ThreadSafe;
import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

/**
 * update to data
 * write changes to commit log
 * update data in ${@link FastSelect} and positions to keep data queryable
 */
@ThreadSafe
public class MutableFastSelect<T extends Item> {

    private static final Logger LOGGER = Logger.getAnonymousLogger();

    private static final String DATA_FILENAME = "data.bin";

    private final boolean useLog;
    private final Map<String, List<Integer>> positions;
    private final CommitLog<T> commitLog;
    private final FastSelect<T> data;
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final File dir;
    private final File dataFile;
    private final ByteData deletedData;
    private final StringData idData;

    public MutableFastSelect(Class<T> clazz, final File dir, final boolean useLog) {
        this.useLog = useLog;
        this.dir = dir;
        this.dataFile = new File(dir, DATA_FILENAME);

        // load data to fast-select
        positions = new HashMap<>();
        data = new FastSelectBuilder<>(clazz).create();
        deletedData = getDeletedData();
        idData = getIdData();

        try {
            data.load(new FileInputStream(dataFile).getChannel(), 5);
        } catch (FileNotFoundException e) {
            // ok, just no data to restore
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        buildPositions();

        // update data with commit log if any
        commitLog = new CommitLog<>(dir, useLog);
        updateDataByCommitLog();

        // todo now we can start to work
//        Thread thread = new Thread(new Runnable() {
//            @Override
//            public void run() {
//                try {
//                    while (true) {
//                        Thread.sleep(TimeUnit.MINUTES.toMillis(5));
//                        flush();
//                    }
//                } catch (InterruptedException e) {
//                    // that's ok just stop thread
//                }
//            }
//        });
//        thread.setDaemon(true);
//        thread.start();
    }

    private void updateDataByCommitLog() {
        for (final DeleteAndAdd<T> deleteAndAdd : commitLog.load()) {
            update(deleteAndAdd);
        }
        commitLog.clear();
    }

    private void update(DeleteAndAdd<T> deleteAndAdd) {
        for (final Integer pos : deleteAndAdd.delete) {
            deletedData.data[pos] = 1;
            String id = (String) idData.get(pos);
            positions.remove(id);
        }

        int i = data.size();
        for (final T obj : deleteAndAdd.add) {
            final String id = obj.getId();
            List<Integer> pos = positions.get(id);
            if (pos == null) {
                pos = new ArrayList<>();
                positions.put(id, pos);
            }
            pos.add(i);
            i++;
        }
        data.addAll(deleteAndAdd.add);
    }

    private ByteData getDeletedData() {
        FastSelect.Column column = data.getColumnsByNames().get("deleted");
        if (column == null)
            throw new IllegalArgumentException("Data object doesn't have 'deleted' column, only: " + data.getColumns());
        return (ByteData) column.data;
    }

    private StringData getIdData() {
        FastSelect.Column column = data.getColumnsByNames().get("id");
        if (column == null)
            throw new IllegalArgumentException("Data object doesn't have 'id' column, only: " + data.getColumns());
        return (StringData) column.data;
    }

    private void buildPositions() {
        // todo impl
    }

    public void select(final Selector<T> selector) {
        final long start = System.currentTimeMillis();
        final Lock lock = readWriteLock.readLock();
        lock.lock();
        try {
            selector.execute(data, positions);
        } finally {
            lock.unlock();
        }
        if (useLog) LOGGER.info("select in " + (System.currentTimeMillis() - start) + " msec");
    }

    public void update(List<Modifier<T>> modifiers) {
        final long start = System.currentTimeMillis();
        final Lock lock = readWriteLock.writeLock();
        lock.lock();
        try {
            DeleteAndAdd<T> deleteAndAdd = new DeleteAndAdd<>(new ArrayList<Integer>(), new ArrayList<T>());
            for (Modifier<T> modifier : modifiers) {
                modifier.execute(deleteAndAdd, data, positions);
            }
            commitLog.write(deleteAndAdd);
            update(deleteAndAdd);
        } finally {
            lock.unlock();
        }
        if (useLog) LOGGER.info("update in " + (System.currentTimeMillis() - start) + " msec");
    }

    public void update(Modifier<T> modifier) {
        final long start = System.currentTimeMillis();
        final Lock lock = readWriteLock.writeLock();
        lock.lock();
        try {
            DeleteAndAdd<T> deleteAndAdd = new DeleteAndAdd<>(new ArrayList<Integer>(), new ArrayList<T>());
            modifier.execute(deleteAndAdd, data, positions);
            commitLog.write(deleteAndAdd);
            update(deleteAndAdd);
        } finally {
            lock.unlock();
        }
        if (useLog) LOGGER.info("update in " + (System.currentTimeMillis() - start) + " msec");
    }

    protected void flush() {
        final Lock lock = readWriteLock.writeLock();
        lock.lock();
        try {
            try {
                data.save(new FileOutputStream(dataFile).getChannel());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            commitLog.clear();
        } finally {
            lock.unlock();
        }
    }

}
