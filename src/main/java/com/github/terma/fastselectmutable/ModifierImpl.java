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

import java.util.List;
import java.util.Map;

public class ModifierImpl<T extends Item> implements Modifier<T> {

    private final List<String> deletes;
    private final List<T> updates;

    public ModifierImpl(List<String> ids, List<T> updates) {
        this.deletes = ids;
        this.updates = updates;
    }

    @Override
    public void execute(DeleteAndAdd<T> acc, FastSelect data, Map<String, List<Integer>> positions) {
        for (String id : deletes) acc.delete.addAll(positions.get(id));
        acc.add.addAll(updates);
    }

}
