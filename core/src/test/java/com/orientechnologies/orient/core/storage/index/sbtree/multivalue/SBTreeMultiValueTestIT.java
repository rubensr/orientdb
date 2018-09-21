package com.orientechnologies.orient.core.storage.index.sbtree.multivalue;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.serialization.types.OUTF8Serializer;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.index.sbtree.local.PrefixBTreeTestIT;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;

public class SBTreeMultiValueTestIT {
  private   OSBTreeMultiValue<String> multiValueTree;
  protected ODatabaseSession          databaseDocumentTx;
  protected String                    buildDirectory;
  protected OrientDB                  orientDB;

  private String dbName;

  @Before
  public void before() {
    buildDirectory = System.getProperty("buildDirectory", ".") + File.separator + PrefixBTreeTestIT.class.getSimpleName();

    dbName = "localMultiBTreeTest";
    final File dbDirectory = new File(buildDirectory, dbName);
    OFileUtils.deleteRecursively(dbDirectory);

    orientDB = new OrientDB("plocal:" + buildDirectory, OrientDBConfig.defaultConfig());
    orientDB.create(dbName, ODatabaseType.PLOCAL);

    databaseDocumentTx = orientDB.open(dbName, "admin", "admin");

    multiValueTree = new OSBTreeMultiValue<>("multiBTree", ".sbt", ".nbt",
        (OAbstractPaginatedStorage) ((ODatabaseInternal) databaseDocumentTx).getStorage());
    multiValueTree.create(OUTF8Serializer.INSTANCE, null, 1, true, null);
  }

  @After
  public void afterMethod() throws Exception {
    orientDB.drop(dbName);
    orientDB.close();
  }

  @Test
  @Ignore
  public void testRandom() {
    final int keysCount = 100_000_000;
    TreeMap<Integer, String> keys = new TreeMap<>();

    for (int i = 0; i < 100; i++) {
      long seed = System.nanoTime();
      System.out.println("Insertion " + i + " is started, seed : " + seed);
      Random random = new Random(seed);

      System.out.println("Generation of keys is started");

      for (int k = 0; k < keysCount; k++) {
        keys.put(k, String.valueOf(k));
      }

      System.out.println("Generation of keys is completed");

      for (int n = 0; n < keysCount; n++) {
        Map.Entry<Integer, String> entry = keys.ceilingEntry(random.nextInt(keysCount));
        if (entry == null) {
          entry = keys.firstEntry();
        }

        multiValueTree.put(entry.getValue(), new ORecordId(entry.getKey() % 32_000, entry.getKey()));
        keys.remove(entry.getKey());
      }

      System.out.println("Insertion " + i + " is completed");
      System.out.println("Check " + i + " is started");

      for (int k = 0; k < keysCount; k++) {
        Assert.assertEquals(multiValueTree.get(String.valueOf(k)), new ORecordId(k % 32_000, k));
      }

      System.out.println("Check " + i + " is completed");
      multiValueTree.delete();

      multiValueTree = new OSBTreeMultiValue<>("multiBTree", ".sbt", ".nbt",
          (OAbstractPaginatedStorage) ((ODatabaseInternal) databaseDocumentTx).getStorage());
      multiValueTree.create(OUTF8Serializer.INSTANCE, null, 1, true, null);
    }
  }

  @Test
  public void testKeyPut() {
    final int keysCount = 100_000_000;

    String lastKey = null;

    for (int i = 0; i < keysCount; i++) {
      final String key = Integer.toString(i);
      multiValueTree.put(key, new ORecordId(i % 32000, i));

      if (i % 100_000 == 0) {
        System.out.printf("%d items loaded out of %d\n", i, keysCount);
      }

      if (lastKey == null) {
        lastKey = key;
      } else if (key.compareTo(lastKey) > 0) {
        lastKey = key;
      }

      Assert.assertEquals("0", multiValueTree.firstKey());
      Assert.assertEquals(lastKey, multiValueTree.lastKey());
    }

    for (int i = 0; i < keysCount; i++) {
      final List<ORID> result = multiValueTree.get(Integer.toString(i));
      Assert.assertEquals(1, result.size());

      Assert.assertTrue(i + " key is absent", result.contains(new ORecordId(i % 32000, i)));
      if (i % 100_000 == 0) {
        System.out.printf("%d items tested out of %d\n", i, keysCount);
      }
    }

    for (int i = keysCount; i < 2 * keysCount; i++) {
      Assert.assertTrue(multiValueTree.get(Integer.toString(i)).isEmpty());
    }
  }

  @Test
  public void testKeyPutRandomUniform() {
    final NavigableMap<String, Integer> keys = new TreeMap<>();
    long seed = System.nanoTime();
    System.out.println("testKeyPutRandomUniform : " + seed);
    final Random random = new Random(seed);
    final int keysCount = 100_000_000;

    while (keys.size() < keysCount) {
      int val = random.nextInt(Integer.MAX_VALUE);
      String key = Integer.toString(val);

      multiValueTree.put(key, new ORecordId(val % 32000, val));
      keys.compute(key, (k, v) -> {
        if (v == null) {
          return 1;
        }

        return v + 1;
      });

      final List<ORID> result = multiValueTree.get(key);
      Assert.assertEquals(keys.get(key).longValue(), result.size());
      final ORID expected = new ORecordId(val % 32000, val);

      for (ORID rid : result) {
        Assert.assertEquals(expected, rid);
      }
    }

    Assert.assertEquals(multiValueTree.firstKey(), keys.firstKey());
    Assert.assertEquals(multiValueTree.lastKey(), keys.lastKey());

    for (Map.Entry<String, Integer> entry : keys.entrySet()) {
      final int val = Integer.parseInt(entry.getKey());
      List<ORID> result = multiValueTree.get(entry.getKey());

      Assert.assertEquals(entry.getValue().longValue(), result.size());
      final ORID expected = new ORecordId(val % 32000, val);

      for (ORID rid : result) {
        Assert.assertEquals(expected, rid);
      }
    }
  }

  @Test
  public void testKeyDelete() {
    final int keysCount = 100_000_000;

    NavigableMap<String, Integer> keys = new TreeMap<>();
    for (int i = 0; i < keysCount; i++) {
      String key = Integer.toString(i);
      multiValueTree.put(key, new ORecordId(i % 32000, i));

      keys.compute(key, (k, v) -> {
        if (v == null) {
          return 1;
        }

        return v + 1;
      });
    }

    Iterator<Map.Entry<String, Integer>> iterator = keys.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<String, Integer> entry = iterator.next();
      String key = entry.getKey();
      int val = Integer.parseInt(key);

      if (val % 3 == 0) {
        multiValueTree.remove(key, new ORecordId(val % 32000, val));
        if (entry.getValue() == 1) {
          iterator.remove();
        } else {
          entry.setValue(entry.getValue() - 1);
        }

      }
    }

    Assert.assertEquals(multiValueTree.firstKey(), keys.firstKey());
    Assert.assertEquals(multiValueTree.lastKey(), keys.lastKey());

    for (Map.Entry<String, Integer> entry : keys.entrySet()) {
      int val = Integer.parseInt(entry.getKey());
      List<ORID> result = multiValueTree.get(entry.getKey());

      Assert.assertEquals(entry.getValue().longValue(), result.size());
      final ORID expected = new ORecordId(val % 32000, val);

      for (ORID rid : result) {
        Assert.assertEquals(expected, rid);
      }
    }
  }

  @Test
  public void testKeyAddDelete() {
    final int keysCount = 100_000_000;

    for (int i = 0; i < keysCount; i++) {
      multiValueTree.put(Integer.toString(i), new ORecordId(i % 32000, i));

      List<ORID> result = multiValueTree.get(Integer.toString(i));
      Assert.assertEquals(1, result.size());
      Assert.assertTrue(result.contains(new ORecordId(i % 32000, i)));
    }

    for (int i = 0; i < keysCount; i++) {
      if (i % 3 == 0) {
        Assert.assertTrue(multiValueTree.remove(Integer.toString(i), new ORecordId(i % 32000, i)));
      }

      if (i % 2 == 0) {
        multiValueTree.put(Integer.toString(keysCount + i), new ORecordId((keysCount + i) % 32000, keysCount + i));
      }

    }

    for (int i = 0; i < keysCount; i++) {
      if (i % 3 == 0) {
        Assert.assertTrue(multiValueTree.get(Integer.toString(i)).isEmpty());
      } else {
        List<ORID> result = multiValueTree.get(Integer.toString(i));

        Assert.assertEquals(1, result.size());
        Assert.assertTrue(result.contains(new ORecordId(i % 32000, i)));
      }

      if (i % 2 == 0) {
        List<ORID> result = multiValueTree.get(Integer.toString(keysCount + i));

        Assert.assertEquals(1, result.size());
        Assert.assertTrue(result.contains(new ORecordId((keysCount + i) % 32000, keysCount + i)));
      }
    }
  }

  @Test
  public void testKeyCursor() {
    final int keysCount = 100_000_000;

    NavigableMap<String, ORID> keyValues = new TreeMap<>();
    final long seed = System.nanoTime();

    System.out.println("testKeyCursor: " + seed);
    Random random = new Random(seed);

    while (keyValues.size() < keysCount) {
      int val = random.nextInt(Integer.MAX_VALUE);
      String key = Integer.toString(val);

      multiValueTree.put(key, new ORecordId(val % 32000, val));
      keyValues.put(key, new ORecordId(val % 32000, val));
    }

    Assert.assertEquals(multiValueTree.firstKey(), keyValues.firstKey());
    Assert.assertEquals(multiValueTree.lastKey(), keyValues.lastKey());

    final OSBTreeMultiValue.OSBTreeKeyCursor<String> cursor = multiValueTree.keyCursor();

    for (String entryKey : keyValues.keySet()) {
      final String indexKey = cursor.next(-1);
      Assert.assertEquals(entryKey, indexKey);
    }
  }

  @Test
  public void testIterateEntriesMajor() {
    final int keysCount = 100_000_000;

    NavigableMap<String, Integer> keyValues = new TreeMap<>();
    final long seed = System.nanoTime();

    System.out.println("testIterateEntriesMajor: " + seed);
    Random random = new Random(seed);

    while (keyValues.size() < keysCount) {
      int val = random.nextInt(Integer.MAX_VALUE);
      String key = Integer.toString(val);

      multiValueTree.put(key, new ORecordId(val % 32000, val));
      keyValues.compute(key, (k, v) -> {
        if (v == null) {
          return 1;
        }

        return v + 1;
      });
    }

    assertIterateMajorEntries(keyValues, random, true, true);
    assertIterateMajorEntries(keyValues, random, false, true);

    assertIterateMajorEntries(keyValues, random, true, false);
    assertIterateMajorEntries(keyValues, random, false, false);

    Assert.assertEquals(multiValueTree.firstKey(), keyValues.firstKey());
    Assert.assertEquals(multiValueTree.lastKey(), keyValues.lastKey());
  }

  @Test
  public void testIterateEntriesMinor() {
    final int keysCount = 100_000_000;
    NavigableMap<String, Integer> keyValues = new TreeMap<>();

    final long seed = System.nanoTime();

    System.out.println("testIterateEntriesMinor: " + seed);
    Random random = new Random(seed);

    while (keyValues.size() < keysCount) {
      int val = random.nextInt(Integer.MAX_VALUE);
      String key = Integer.toString(val);

      multiValueTree.put(key, new ORecordId(val % 32000, val));
      keyValues.compute(key, (k, v) -> {
        if (v == null) {
          return 1;
        }

        return v + 1;
      });
    }

    assertIterateMinorEntries(keyValues, random, true, true);
    assertIterateMinorEntries(keyValues, random, false, true);

    assertIterateMinorEntries(keyValues, random, true, false);
    assertIterateMinorEntries(keyValues, random, false, false);

    Assert.assertEquals(multiValueTree.firstKey(), keyValues.firstKey());
    Assert.assertEquals(multiValueTree.lastKey(), keyValues.lastKey());
  }

  @Test
  public void testIterateEntriesBetween() {
    final int keysCount = 100_000_000;
    NavigableMap<String, Integer> keyValues = new TreeMap<>();
    Random random = new Random();

    while (keyValues.size() < keysCount) {
      int val = random.nextInt(Integer.MAX_VALUE);
      String key = Integer.toString(val);

      multiValueTree.put(key, new ORecordId(val % 32000, val));
      keyValues.compute(key, (k, v) -> {
        if (v == null) {
          return 1;
        }

        return v + 1;
      });
    }

    assertIterateBetweenEntries(keyValues, random, true, true, true);
    assertIterateBetweenEntries(keyValues, random, true, false, true);
    assertIterateBetweenEntries(keyValues, random, false, true, true);
    assertIterateBetweenEntries(keyValues, random, false, false, true);

    assertIterateBetweenEntries(keyValues, random, true, true, false);
    assertIterateBetweenEntries(keyValues, random, true, false, false);
    assertIterateBetweenEntries(keyValues, random, false, true, false);
    assertIterateBetweenEntries(keyValues, random, false, false, false);

    Assert.assertEquals(multiValueTree.firstKey(), keyValues.firstKey());
    Assert.assertEquals(multiValueTree.lastKey(), keyValues.lastKey());
  }

  private void assertIterateMajorEntries(NavigableMap<String, Integer> keyValues, Random random, boolean keyInclusive,
      boolean ascSortOrder) {
    String[] keys = new String[keyValues.size()];
    int index = 0;

    for (String key : keyValues.keySet()) {
      keys[index] = key;
      index++;
    }

    for (int i = 0; i < 100; i++) {
      final int fromKeyIndex = random.nextInt(keys.length);
      String fromKey = keys[fromKeyIndex];

      if (random.nextBoolean()) {
        fromKey = fromKey.substring(0, fromKey.length() - 2) + (fromKey.charAt(fromKey.length() - 1) - 1);
      }

      final OSBTreeMultiValue.OSBTreeCursor<String, ORID> cursor = multiValueTree
          .iterateEntriesMajor(fromKey, keyInclusive, ascSortOrder);

      Iterator<Map.Entry<String, Integer>> iterator;
      if (ascSortOrder) {
        iterator = keyValues.tailMap(fromKey, keyInclusive).entrySet().iterator();
      } else {
        iterator = keyValues.descendingMap().subMap(keyValues.lastKey(), true, fromKey, keyInclusive).entrySet().iterator();
      }

      while (iterator.hasNext()) {
        Map.Entry<String, ORID> indexEntry = cursor.next(-1);
        final Map.Entry<String, Integer> entry = iterator.next();

        final int repetition = entry.getValue();
        final int value = Integer.parseInt(entry.getKey());
        final ORID expected = new ORecordId(value % 32_000, value);

        Assert.assertEquals(entry.getKey(), indexEntry.getKey());
        Assert.assertEquals(expected, indexEntry.getValue());

        for (int n = 1; n < repetition; n++) {
          indexEntry = cursor.next(-1);

          Assert.assertEquals(entry.getKey(), indexEntry.getKey());
          Assert.assertEquals(expected, indexEntry.getValue());
        }
      }

      Assert.assertFalse(iterator.hasNext());
      Assert.assertNull(cursor.next(-1));
    }
  }

  private void assertIterateMinorEntries(NavigableMap<String, Integer> keyValues, Random random, boolean keyInclusive,
      boolean ascSortOrder) {
    String[] keys = new String[keyValues.size()];
    int index = 0;

    for (String key : keyValues.keySet()) {
      keys[index] = key;
      index++;
    }

    for (int i = 0; i < 100; i++) {
      int toKeyIndex = random.nextInt(keys.length);
      String toKey = keys[toKeyIndex];
      if (random.nextBoolean()) {
        toKey = toKey.substring(0, toKey.length() - 2) + (toKey.charAt(toKey.length() - 1) + 1);
      }

      final OSBTreeMultiValue.OSBTreeCursor<String, ORID> cursor = multiValueTree
          .iterateEntriesMinor(toKey, keyInclusive, ascSortOrder);

      Iterator<Map.Entry<String, Integer>> iterator;
      if (ascSortOrder) {
        iterator = keyValues.headMap(toKey, keyInclusive).entrySet().iterator();
      } else {
        iterator = keyValues.headMap(toKey, keyInclusive).descendingMap().entrySet().iterator();
      }

      while (iterator.hasNext()) {
        Map.Entry<String, ORID> indexEntry = cursor.next(-1);
        Map.Entry<String, Integer> entry = iterator.next();

        final int repetition = entry.getValue();
        final int value = Integer.parseInt(entry.getKey());
        final ORID expected = new ORecordId(value % 32_000, value);

        Assert.assertEquals(entry.getKey(), indexEntry.getKey());
        Assert.assertEquals(expected, indexEntry.getValue());

        for (int n = 1; n < repetition; n++) {
          indexEntry = cursor.next(-1);

          Assert.assertEquals(entry.getKey(), indexEntry.getKey());
          Assert.assertEquals(expected, indexEntry.getValue());
        }
      }

      Assert.assertFalse(iterator.hasNext());
      Assert.assertNull(cursor.next(-1));
    }
  }

  private void assertIterateBetweenEntries(NavigableMap<String, Integer> keyValues, Random random, boolean fromInclusive,
      boolean toInclusive, boolean ascSortOrder) {
    String[] keys = new String[keyValues.size()];
    int index = 0;

    for (String key : keyValues.keySet()) {
      keys[index] = key;
      index++;
    }

    for (int i = 0; i < 100; i++) {
      int fromKeyIndex = random.nextInt(keys.length);
      int toKeyIndex = random.nextInt(keys.length);

      if (fromKeyIndex > toKeyIndex) {
        toKeyIndex = fromKeyIndex;
      }

      String fromKey = keys[fromKeyIndex];
      String toKey = keys[toKeyIndex];

      if (random.nextBoolean()) {
        fromKey = fromKey.substring(0, fromKey.length() - 2) + (fromKey.charAt(fromKey.length() - 1) - 1);
      }

      if (random.nextBoolean()) {
        toKey = toKey.substring(0, toKey.length() - 2) + (toKey.charAt(toKey.length() - 1) + 1);
      }

      if (fromKey.compareTo(toKey) > 0) {
        fromKey = toKey;
      }

      OSBTreeMultiValue.OSBTreeCursor<String, ORID> cursor = multiValueTree
          .iterateEntriesBetween(fromKey, fromInclusive, toKey, toInclusive, ascSortOrder);

      Iterator<Map.Entry<String, Integer>> iterator;
      if (ascSortOrder) {
        iterator = keyValues.subMap(fromKey, fromInclusive, toKey, toInclusive).entrySet().iterator();
      } else {
        iterator = keyValues.descendingMap().subMap(toKey, toInclusive, fromKey, fromInclusive).entrySet().iterator();
      }

      while (iterator.hasNext()) {
        Map.Entry<String, ORID> indexEntry = cursor.next(-1);
        Assert.assertNotNull(indexEntry);

        Map.Entry<String, Integer> entry = iterator.next();

        final int repetition = entry.getValue();
        final int value = Integer.parseInt(entry.getKey());
        final ORID expected = new ORecordId(value % 32_000, value);

        Assert.assertEquals(entry.getKey(), indexEntry.getKey());
        Assert.assertEquals(expected, indexEntry.getValue());

        for (int n = 1; n < repetition; n++) {
          indexEntry = cursor.next(-1);

          Assert.assertEquals(entry.getKey(), indexEntry.getKey());
          Assert.assertEquals(expected, indexEntry.getValue());
        }
      }
      Assert.assertFalse(iterator.hasNext());
      Assert.assertNull(cursor.next(-1));
    }
  }

}
