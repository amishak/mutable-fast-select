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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DeleteAndAdd<T> implements Serializable {

    public final List<Integer> delete;
    public final List<T> add;

    public DeleteAndAdd(List<Integer> delete, List<T> add) {
        this.delete = new ArrayList<>(delete);
        this.add = new ArrayList<>(add);
    }

    /**
     * only for {@link com.esotericsoftware.kryo.Kryo}
     */
    private DeleteAndAdd() {
        delete = Collections.emptyList();
        add = Collections.emptyList();
    }

}
