# mutable-fast-select

[![Build Status](https://travis-ci.org/terma/mutable-fast-select.svg)](https://travis-ci.org/terma/mutable-fast-select) [![Coverage Status](https://coveralls.io/repos/terma/mutable-fast-select/badge.svg?branch=master&service=github)](https://coveralls.io/github/terma/mutable-fast-select?branch=master) [![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.terma.fastselectmutable/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.github.terma.fastselectmutable/)

Mutable version of fast-select with persistence.

## Usage

Add dependency:

```xml
<dependency>
  <groupId>com.github.terma</groupId>
  <artifactId>mutable-fast-select</artifactId>
  <version>0.0.6</version>
</dependency>
```

Create instance of ```mutable-fast-select```
```java
File dir = new File("???"); // path to dir where state will be stored
boolean useLogging = false;
MutableFastSelect<Data> m = new MutableFastSelect<>(Data.class, dir, useLogging);
```

Modify data, like simple update/insert:

```java
Data data1 = ...;
Data data2 = ...;
m.modify(Updater.update(data1, data2));
```

Query:
```java
mutableFastSelect.select(new Selector<Data> {
                                                        
  @Override
  public void execute(FastSelect<Data> data, Map<Object, List<Integer>> positions) {
    // some work on fast-select (data)
  }
  
});
```

## Dependencies

- [fast-select](https://github.com/terma/fast-select)