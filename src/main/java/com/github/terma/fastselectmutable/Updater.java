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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Basic implementation of {@link Modifier} to add, delete or update data in {@link MutableFastSelect}
 *
 * @param <T>
 */
@SuppressWarnings("WeakerAccess")
public class Updater<T extends Item> implements Modifier<T> {

    private final List<String> deletes;
    private final List<T> updates;

    public Updater(final List<String> ids, final List<T> updates) {
        this.deletes = ids;
        this.updates = updates;
    }

    public static <T extends Item> Updater<T> delete(final String... ids) {
        return new Updater<>(Arrays.asList(ids), Collections.<T>emptyList());
    }

    public static <T extends Item> Updater<T> update(final T... updates) {
        return new Updater<>(Collections.<String>emptyList(), Arrays.asList(updates));
    }

    @Override
    public void execute(final DeleteAndAdd<T> acc, final FastSelect data, final Map<Object, List<Integer>> positions) {
        for (T i : updates) {
            List<Integer> list = positions.get(i.getId());
            if (list != null) acc.delete.addAll(list);
        }

        for (String id : deletes) acc.delete.addAll(positions.get(id));
        acc.add.addAll(updates);
    }

}
