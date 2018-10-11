package com.orientechnologies.orient.core.index.engine.v1;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.encryption.OEncryption;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndexAbstractCursor;
import com.orientechnologies.orient.core.index.OIndexCursor;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexKeyCursor;
import com.orientechnologies.orient.core.index.engine.OSingleValueIndexEngine;
import com.orientechnologies.orient.core.iterator.OEmptyIterator;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.index.sbtree.singlevalue.OSBTreeSingleValue;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class OSBTreeSingleValueIndexEngine implements OSingleValueIndexEngine {
  public static final String DATA_FILE_EXTENSION        = ".sbt";
  public static final String NULL_BUCKET_FILE_EXTENSION = ".nbt";

  private final OSBTreeSingleValue<Object> sbTree;
  private final String                     name;

  public OSBTreeSingleValueIndexEngine(String name, OAbstractPaginatedStorage storage) {
    this.name = name;
    this.sbTree = new OSBTreeSingleValue<>(name, DATA_FILE_EXTENSION, NULL_BUCKET_FILE_EXTENSION, storage);
  }

  @Override
  public void init(String indexName, String indexType, OIndexDefinition indexDefinition, boolean isAutomatic, ODocument metadata) {
  }

  @Override
  public void flush() {
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public void create(OBinarySerializer valueSerializer, boolean isAutomatic, OType[] keyTypes, boolean nullPointerSupport,
      OBinarySerializer keySerializer, int keySize, Set<String> clustersToIndex, Map<String, String> engineProperties,
      ODocument metadata, OEncryption encryption) {
    //noinspection unchecked
    sbTree.create(keySerializer, keyTypes, keySize, nullPointerSupport, encryption);
  }

  @Override
  public void delete() {
    sbTree.delete();
  }

  @Override
  public void deleteWithoutLoad(String indexName) {
    sbTree.deleteWithoutLoad();
  }

  @Override
  public void load(String indexName, OBinarySerializer valueSerializer, boolean isAutomatic, OBinarySerializer keySerializer,
      OType[] keyTypes, boolean nullPointerSupport, int keySize, Map<String, String> engineProperties, OEncryption encryption) {
    //noinspection unchecked
    sbTree.load(indexName, keySerializer, keyTypes, keySize, nullPointerSupport, encryption);
  }

  @Override
  public boolean contains(Object key) {
    return sbTree.get(key) != null;
  }

  @Override
  public boolean remove(Object key) {
    return sbTree.remove(key) != null;
  }

  @Override
  public int getVersion() {
    return 0;
  }

  @Override
  public void clear() {
    sbTree.clear();
  }

  @Override
  public void close() {
    sbTree.close();
  }

  @Override
  public ORID get(Object key) {
    return sbTree.get(key);
  }

  @Override
  public OIndexCursor cursor(ValuesTransformer valuesTransformer) {
    final Object firstKey = sbTree.firstKey();
    if (firstKey == null) {
      return new NullCursor();
    }

    return new OSBTreeIndexCursor(sbTree.iterateEntriesMajor(firstKey, true, true), valuesTransformer);
  }

  @Override
  public OIndexCursor descCursor(ValuesTransformer valuesTransformer) {
    final Object lastKey = sbTree.lastKey();
    if (lastKey == null) {
      return new NullCursor();
    }

    return new OSBTreeIndexCursor(sbTree.iterateEntriesMinor(lastKey, true, false), valuesTransformer);
  }

  @Override
  public OIndexKeyCursor keyCursor() {
    return new OIndexKeyCursor() {
      private final OSBTreeSingleValue.OSBTreeKeyCursor<Object> sbTreeKeyCursor = sbTree.keyCursor();

      @Override
      public Object next(int prefetchSize) {
        return sbTreeKeyCursor.next(prefetchSize);
      }
    };
  }

  @Override
  public void put(Object key, ORID value) {
    sbTree.put(key, value);
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean validatedPut(Object key, ORID value, Validator<Object, ORID> validator) {
    return sbTree.validatedPut(key, value, validator);
  }

  @Override
  public Object getFirstKey() {
    return sbTree.firstKey();
  }

  @Override
  public Object getLastKey() {
    return sbTree.lastKey();
  }

  @Override
  public OIndexCursor iterateEntriesBetween(Object rangeFrom, boolean fromInclusive, Object rangeTo, boolean toInclusive,
      boolean ascSortOrder, ValuesTransformer transformer) {
    return new OSBTreeIndexCursor(sbTree.iterateEntriesBetween(rangeFrom, fromInclusive, rangeTo, toInclusive, ascSortOrder),
        transformer);
  }

  @Override
  public OIndexCursor iterateEntriesMajor(Object fromKey, boolean isInclusive, boolean ascSortOrder,
      ValuesTransformer transformer) {
    return new OSBTreeIndexCursor(sbTree.iterateEntriesMajor(fromKey, isInclusive, ascSortOrder), transformer);
  }

  @Override
  public OIndexCursor iterateEntriesMinor(Object toKey, boolean isInclusive, boolean ascSortOrder, ValuesTransformer transformer) {
    return new OSBTreeIndexCursor(sbTree.iterateEntriesMinor(toKey, isInclusive, ascSortOrder), transformer);
  }

  @Override
  public long size(final ValuesTransformer transformer) {
    if (transformer == null) {
      return sbTree.size();
    } else {
      int counter = 0;

      if (sbTree.isNullPointerSupport()) {
        final Object nullValue = sbTree.get(null);
        if (nullValue != null) {
          counter += transformer.transformFromValue(nullValue).size();
        }
      }

      final Object firstKey = sbTree.firstKey();
      final Object lastKey = sbTree.lastKey();

      if (firstKey != null && lastKey != null) {
        final OSBTreeSingleValue.OSBTreeCursor<Object, ORID> cursor = sbTree
            .iterateEntriesBetween(firstKey, true, lastKey, true, true);
        Map.Entry<Object, ORID> entry = cursor.next(-1);
        while (entry != null) {
          counter += transformer.transformFromValue(entry.getValue()).size();
          entry = cursor.next(-1);
        }

        return counter;
      }

      return counter;
    }
  }

  @Override
  public boolean hasRangeQuerySupport() {
    return true;
  }

  @Override
  public boolean acquireAtomicExclusiveLock(Object key) {
    sbTree.acquireAtomicExclusiveLock();
    return true;
  }

  @Override
  public String getIndexNameByKey(Object key) {
    return name;
  }

  private static final class OSBTreeIndexCursor extends OIndexAbstractCursor {
    private final OSBTreeSingleValue.OSBTreeCursor<Object, ORID> treeCursor;
    private final ValuesTransformer                              valuesTransformer;

    private Iterator<ORID> currentIterator = OEmptyIterator.IDENTIFIABLE_INSTANCE;
    private Object         currentKey      = null;

    private OSBTreeIndexCursor(OSBTreeSingleValue.OSBTreeCursor<Object, ORID> treeCursor, ValuesTransformer valuesTransformer) {
      this.treeCursor = treeCursor;
      this.valuesTransformer = valuesTransformer;
    }

    @Override
    public Map.Entry<Object, OIdentifiable> nextEntry() {
      if (valuesTransformer == null) {
        final Object entry = treeCursor.next(getPrefetchSize());
        //noinspection unchecked
        return (Map.Entry<Object, OIdentifiable>) entry;
      }

      if (currentIterator == null) {
        return null;
      }

      while (!currentIterator.hasNext()) {
        final Object p = treeCursor.next(getPrefetchSize());
        @SuppressWarnings("unchecked")
        final Map.Entry<Object, OIdentifiable> entry = (Map.Entry<Object, OIdentifiable>) p;

        if (entry == null) {
          currentIterator = null;
          return null;
        }

        currentKey = entry.getKey();
        currentIterator = valuesTransformer.transformFromValue(entry.getValue()).iterator();
      }

      final OIdentifiable value = currentIterator.next();

      return new Map.Entry<Object, OIdentifiable>() {
        @Override
        public Object getKey() {
          return currentKey;
        }

        @Override
        public OIdentifiable getValue() {
          return value;
        }

        @Override
        public OIdentifiable setValue(OIdentifiable value) {
          throw new UnsupportedOperationException("setValue");
        }
      };
    }
  }

  private static class NullCursor extends OIndexAbstractCursor {
    @Override
    public Map.Entry<Object, OIdentifiable> nextEntry() {
      return null;
    }
  }
}
