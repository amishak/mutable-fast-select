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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;

@SuppressWarnings("WeakerAccess")
public class CommitLog<T> {

    private static final Logger LOGGER = Logger.getAnonymousLogger();

    private static final String FILENAME = "commit-log.bin";

    private final boolean useLog;
    private final Kryo kryo = new Kryo();
    private final File file;
    private FileChannel fileChannel;

    public CommitLog(final File dir, final boolean useLog) {
        this.useLog = useLog;
        this.file = new File(dir, FILENAME);
        try {
            fileChannel = new FileOutputStream(file).getChannel();
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        kryo.register(DeleteAndAdd.class);
    }

    public void write(List<DeleteAndAdd<T>> updates) {
        try {
            final long start = System.currentTimeMillis();
            long s = 0;

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            Output oos = new Output(baos);
            for (DeleteAndAdd<T> update : updates) {
                kryo.writeObject(oos, update);
            }
            oos.close();
            byte[] array = baos.toByteArray();
            s += array.length;
            fileChannel.write(ByteBuffer.wrap(array));
            fileChannel.force(false);
            if (useLog)
                LOGGER.info("write " + updates.size() + " as " + (s / 1024) + " kb in " + (System.currentTimeMillis() - start) + " msec");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void write(DeleteAndAdd<T> update) {
        try {
            final long start = System.currentTimeMillis();
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            Output oos = new Output(baos);
            kryo.writeObject(oos, update);
            oos.close();
            byte[] array = baos.toByteArray();
            fileChannel.write(ByteBuffer.wrap(array));
            fileChannel.force(false);
            if (useLog)
                LOGGER.info("write " + (array.length / 1024) + " kb in " + (System.currentTimeMillis() - start) + " msec");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void clear() {
        try {
            fileChannel.position(0);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Iterable<DeleteAndAdd<T>> load() {
        return Collections.emptyList(); // todo
    }

}
