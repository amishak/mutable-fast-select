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
import java.nio.channels.FileChannel;
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
@SuppressWarnings("WeakerAccess")
@ThreadSafe
public class MutableFastSelect<T extends Item> {

    private static final Logger LOGGER = Logger.getAnonymousLogger();

    private static final long COMMIT_LOG_THRESHOLD = 10 * 1024 * 1024;
    private static final int LOAD_THREADS = 5;

    private static final String DATA_FILENAME = "data.bin";

    private final long commitLogThreshold;
    private final boolean useLog;

    private final Map<String, List<Integer>> positions;
    private final CommitLog<T> commitLog;
    private final FastSelect<T> data;
    private final File dataFile;

    private final ByteData deletedData;
    private final StringData idData;

    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final Lock r = readWriteLock.readLock();
    private final Lock w = readWriteLock.writeLock();

    public MutableFastSelect(Class<T> clazz, final File dir, final boolean useLog) {
        this(clazz, dir, useLog, COMMIT_LOG_THRESHOLD);
    }

    /**
     * @param clazz              - data class
     * @param dir                - directory where commit log and data file will be stored
     * @param useLog             - enable logging
     * @param commitLogThreshold - max size of {@link CommitLog} in bytes before it it
     *                           will be flushed to {@link FastSelect#save(FileChannel)}
     */
    public MutableFastSelect(Class<T> clazz, final File dir, final boolean useLog, final long commitLogThreshold) {
        this.commitLogThreshold = commitLogThreshold;
        this.useLog = useLog;
        this.dataFile = new File(dir, DATA_FILENAME);

        // load data to fast-select
        positions = new HashMap<>();
        data = new FastSelectBuilder<>(clazz).create();

        final FastSelect.Column deleteColumn = data.getColumnsByNames().get("deleted");
        if (deleteColumn == null)
            throw new IllegalArgumentException("Data object doesn't have 'deleted' column, only: " + data.getColumns());
        deletedData = (ByteData) deleteColumn.data;

        final FastSelect.Column idColumn = data.getColumnsByNames().get("id");
        if (idColumn == null)
            throw new IllegalArgumentException("Data object doesn't have 'id' column, only: " + data.getColumns());
        idData = (StringData) idColumn.data;

        try (final FileChannel fileChannel = new FileInputStream(dataFile).getChannel()) {
            data.load(fileChannel, LOAD_THREADS);
        } catch (FileNotFoundException e) {
            // ok, just no data to restore
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // build positions map
        for (int i = 0; i < idData.size(); i++) {
            String id = (String) idData.get(i);
            List<Integer> pos = positions.get(id);
            if (pos == null) {
                pos = new ArrayList<>();
                positions.put(id, pos);
            }
            pos.add(i);
        }

        // update data with commit log if any
        commitLog = new CommitLog<>(dir, useLog);
        for (final DeleteAndAdd<T> deleteAndAdd : commitLog.load()) update(deleteAndAdd);
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

    public void select(final Selector<T> selector) {
        final long start = System.currentTimeMillis();
        r.lock();
        try {
            selector.execute(data, positions);
        } finally {
            r.unlock();
        }
        if (useLog) LOGGER.info("select in " + (System.currentTimeMillis() - start) + " msec");
    }

    public void update(final Modifier<T> modifier) {
        final long start = System.currentTimeMillis();
        w.lock();
        try {
            DeleteAndAdd<T> deleteAndAdd = new DeleteAndAdd<>(new ArrayList<Integer>(), new ArrayList<T>());
            modifier.execute(deleteAndAdd, data, positions);
            commitLog.write(deleteAndAdd);
            update(deleteAndAdd);

            if (commitLog.size() > commitLogThreshold) flushCommitLog();
        } finally {
            w.unlock();
        }
        if (useLog) LOGGER.info("update in " + (System.currentTimeMillis() - start) + " msec");
    }

    private void flushCommitLog() {
        try (final FileChannel fileChannel = new RandomAccessFile(dataFile, "rw").getChannel()) {
            data.save(fileChannel);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        commitLog.clear();
    }

}
