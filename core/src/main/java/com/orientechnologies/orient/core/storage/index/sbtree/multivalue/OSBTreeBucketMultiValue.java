/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.orient.core.storage.index.sbtree.multivalue;

import com.orientechnologies.common.comparator.ODefaultComparator;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.serialization.types.OShortSerializer;
import com.orientechnologies.orient.core.encryption.OEncryption;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 8/7/13
 */
public class OSBTreeBucketMultiValue<K> extends ODurablePage {
  private static final int RID_SIZE              = OShortSerializer.SHORT_SIZE + OLongSerializer.LONG_SIZE;
  private static final int LINKED_LIST_ITEM_SIZE = RID_SIZE + OIntegerSerializer.INT_SIZE;

  private static final int FREE_POINTER_OFFSET  = NEXT_FREE_POSITION;
  private static final int SIZE_OFFSET          = FREE_POINTER_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int IS_LEAF_OFFSET       = SIZE_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int LEFT_SIBLING_OFFSET  = IS_LEAF_OFFSET + OByteSerializer.BYTE_SIZE;
  private static final int RIGHT_SIBLING_OFFSET = LEFT_SIBLING_OFFSET + OLongSerializer.LONG_SIZE;

  private static final int TREE_SIZE_OFFSET       = RIGHT_SIBLING_OFFSET + OLongSerializer.LONG_SIZE;
  private static final int POSITIONS_ARRAY_OFFSET = TREE_SIZE_OFFSET + OLongSerializer.LONG_SIZE;

  private final boolean isLeaf;

  private final OBinarySerializer<K> keySerializer;

  private final Comparator<? super K> comparator = ODefaultComparator.INSTANCE;

  private final OEncryption encryption;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  OSBTreeBucketMultiValue(OCacheEntry cacheEntry, boolean isLeaf, OBinarySerializer<K> keySerializer, OEncryption encryption) {
    super(cacheEntry);

    this.isLeaf = isLeaf;
    this.keySerializer = keySerializer;
    this.encryption = encryption;

    setIntValue(FREE_POINTER_OFFSET, MAX_PAGE_SIZE_BYTES);
    setIntValue(SIZE_OFFSET, 0);

    setByteValue(IS_LEAF_OFFSET, (byte) (isLeaf ? 1 : 0));
    setLongValue(LEFT_SIBLING_OFFSET, -1);
    setLongValue(RIGHT_SIBLING_OFFSET, -1);

    setLongValue(TREE_SIZE_OFFSET, 0);
  }

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  OSBTreeBucketMultiValue(OCacheEntry cacheEntry, OBinarySerializer<K> keySerializer, OEncryption encryption) {
    super(cacheEntry);
    this.encryption = encryption;

    this.isLeaf = getByteValue(IS_LEAF_OFFSET) > 0;
    this.keySerializer = keySerializer;
  }

  void setTreeSize(long size) {
    setLongValue(TREE_SIZE_OFFSET, size);
  }

  long getTreeSize() {
    return getLongValue(TREE_SIZE_OFFSET);
  }

  boolean isEmpty() {
    return size() == 0;
  }

  int find(K key) {
    int low = 0;
    int high = size() - 1;

    while (low <= high) {
      int mid = (low + high) >>> 1;
      K midVal = getKey(mid);
      int cmp = comparator.compare(midVal, key);

      if (cmp < 0) {
        low = mid + 1;
      } else if (cmp > 0) {
        high = mid - 1;
      } else {
        return mid; // key found
      }
    }

    return -(low + 1); // key not found.
  }

  int remove(final int entryIndex) {
    assert isLeaf;

    final int entryPosition = getIntValue(POSITIONS_ARRAY_OFFSET + entryIndex * OIntegerSerializer.INT_SIZE);

    int position = entryPosition;
    int nextItem = getIntValue(position);
    position += OIntegerSerializer.INT_SIZE;

    final int keySize;
    if (encryption == null) {
      keySize = getObjectSizeInDirectMemory(keySerializer, position);
    } else {
      final int encryptedSize = getIntValue(position);
      keySize = encryptedSize + OIntegerSerializer.INT_SIZE;
    }

    if (nextItem == -1) {
      removeMainEntry(entryIndex, entryPosition, keySize);

      return 1;
    }

    List<Integer> itemsToRemove = new ArrayList<>();
    final int entrySize = keySize + OIntegerSerializer.INT_SIZE + RID_SIZE;
    int totalSpace = entrySize;

    while (nextItem > 0) {
      itemsToRemove.add(nextItem);
      nextItem = getIntValue(nextItem);
    }

    totalSpace += itemsToRemove.size() * LINKED_LIST_ITEM_SIZE;

    int size = getIntValue(SIZE_OFFSET);

    final TreeMap<Integer, Integer> entries = new TreeMap<>();
    final ConcurrentSkipListMap<Integer, Integer> nexts = new ConcurrentSkipListMap<>();

    int currentPositionOffset = POSITIONS_ARRAY_OFFSET;

    for (int i = 0; i < size; i++) {
      if (i != entryIndex) {
        final int currentEntryPosition = getIntValue(currentPositionOffset);
        final int currentNextPosition = getIntValue(currentEntryPosition);

        entries.put(currentEntryPosition, currentPositionOffset);
        if (currentNextPosition > 0) {
          nexts.put(currentNextPosition, currentEntryPosition);
        }
      }

      currentPositionOffset += OIntegerSerializer.INT_SIZE;
    }

    int freeSpacePointer = getIntValue(FREE_POINTER_OFFSET);

    int counter = 0;
    for (int itemToRemove : itemsToRemove) {
      if (itemToRemove > freeSpacePointer) {
        moveData(freeSpacePointer, freeSpacePointer + LINKED_LIST_ITEM_SIZE, itemToRemove - freeSpacePointer);

        final SortedMap<Integer, Integer> linkRefToCorrect = nexts.headMap(itemToRemove);
        final int diff = totalSpace - counter * LINKED_LIST_ITEM_SIZE;

        final SortedMap<Integer, Integer> entriesRefToCorrect = entries.headMap(itemToRemove);
        for (Map.Entry<Integer, Integer> entry : entriesRefToCorrect.entrySet()) {
          final int currentEntryOffset = entry.getValue();
          final int currentEntryPosition = entry.getKey();

          setIntValue(currentEntryOffset, currentEntryPosition + diff);
        }

        entriesRefToCorrect.clear();

        for (Map.Entry<Integer, Integer> entry : linkRefToCorrect.entrySet()) {
          final int first = entry.getKey();
          final int currentEntryPosition = entry.getValue();

          if (first < itemToRemove) {
            if (currentEntryPosition > 0) {
              final int updatedEntryPosition;
              if (currentEntryPosition < itemToRemove) {
                final int itemsBefore = -Collections.binarySearch(itemsToRemove, currentEntryPosition) - 1;
                if (counter >= itemsBefore) {
                  updatedEntryPosition = currentEntryPosition + (counter - itemsBefore + 1) * LINKED_LIST_ITEM_SIZE;
                } else {
                  updatedEntryPosition = currentEntryPosition;
                }
              } else {
                updatedEntryPosition = currentEntryPosition;
              }

              setIntValue(updatedEntryPosition, first + diff);
            } else {
              final int compositeEntryPosition = -currentEntryPosition;
              final int prevCounter = compositeEntryPosition >>> 16;
              final int prevEntryPosition = compositeEntryPosition & 0xFFFF;

              final int updatedEntryPosition = (counter - prevCounter) * LINKED_LIST_ITEM_SIZE + prevEntryPosition;

              setIntValue(updatedEntryPosition, first + diff);
            }
          }

          final int[] lastEntry = updateAllLinkedListReferences(first, itemToRemove, LINKED_LIST_ITEM_SIZE, diff);

          if (lastEntry[1] > 0) {
            nexts.put(lastEntry[1], -((counter << 16) | lastEntry[0]));
          }
        }

        linkRefToCorrect.clear();

      }

      counter++;
      freeSpacePointer += LINKED_LIST_ITEM_SIZE;
    }

    if (entryPosition > freeSpacePointer) {
      moveData(freeSpacePointer, freeSpacePointer + entrySize, entryPosition - freeSpacePointer);

      final SortedMap<Integer, Integer> linkRefToCorrect = nexts.headMap(entryPosition);
      final int diff = entrySize;

      final SortedMap<Integer, Integer> entriesRefToCorrect = entries.headMap(entryPosition);
      for (Map.Entry<Integer, Integer> entry : entriesRefToCorrect.entrySet()) {
        final int currentEntryOffset = entry.getValue();
        final int currentEntryPosition = entry.getKey();

        setIntValue(currentEntryOffset, currentEntryPosition + diff);
      }

      for (Map.Entry<Integer, Integer> entry : linkRefToCorrect.entrySet()) {
        final int first = entry.getKey();
        final int currentEntryPosition = entry.getValue();

        if (currentEntryPosition > 0) {
          int updatedEntryPosition;

          final int itemsBefore = -Collections.binarySearch(itemsToRemove, currentEntryPosition) - 1;
          if (itemsToRemove.size() > itemsBefore) {
            updatedEntryPosition = currentEntryPosition + (itemsToRemove.size() - itemsBefore) * LINKED_LIST_ITEM_SIZE;
          } else {
            updatedEntryPosition = currentEntryPosition;
          }

          if (currentEntryPosition < entryPosition) {
            updatedEntryPosition += entrySize;
          }

          setIntValue(updatedEntryPosition, first + diff);
        } else {
          final int compositeEntryPosition = -currentEntryPosition;
          final int prevCounter = compositeEntryPosition >>> 16;
          final int prevEntryPosition = compositeEntryPosition & 0xFFFF;

          final int updatedEntryPosition =
              entrySize + (itemsToRemove.size() - prevCounter - 1) * LINKED_LIST_ITEM_SIZE + prevEntryPosition;

          setIntValue(updatedEntryPosition, first + diff);
        }

        updateAllLinkedListReferences(first, entryPosition, diff);
      }
    }

    freeSpacePointer += entrySize;

    setIntValue(FREE_POINTER_OFFSET, freeSpacePointer);

    if (entryIndex < size) {
      moveData(POSITIONS_ARRAY_OFFSET + (entryIndex + 1) * OIntegerSerializer.INT_SIZE,
          POSITIONS_ARRAY_OFFSET + entryIndex * OIntegerSerializer.INT_SIZE, (size - entryIndex - 1) * OIntegerSerializer.INT_SIZE);
    }

    size--;
    setIntValue(SIZE_OFFSET, size);

    return itemsToRemove.size() + 1;
  }

  private void removeMainEntry(int entryIndex, int entryPosition, int keySize) {
    int nextItem;
    int size = getIntValue(SIZE_OFFSET);

    if (entryIndex < size - 1) {
      moveData(POSITIONS_ARRAY_OFFSET + (entryIndex + 1) * OIntegerSerializer.INT_SIZE,
          POSITIONS_ARRAY_OFFSET + entryIndex * OIntegerSerializer.INT_SIZE, (size - entryIndex - 1) * OIntegerSerializer.INT_SIZE);
    }

    size--;
    setIntValue(SIZE_OFFSET, size);

    final int freePointer = getIntValue(FREE_POINTER_OFFSET);
    final int entrySize = OIntegerSerializer.INT_SIZE + RID_SIZE + keySize;

    boolean moved = false;
    if (size > 0 && entryPosition > freePointer) {
      moveData(freePointer, freePointer + entrySize, entryPosition - freePointer);
      moved = true;
    }

    setIntValue(FREE_POINTER_OFFSET, freePointer + entrySize);

    if (moved) {
      int currentPositionOffset = POSITIONS_ARRAY_OFFSET;

      for (int i = 0; i < size; i++) {
        final int currentEntryPosition = getIntValue(currentPositionOffset);
        final int updatedEntryPosition;

        if (currentEntryPosition < entryPosition) {
          updatedEntryPosition = currentEntryPosition + entrySize;
          setIntValue(currentPositionOffset, updatedEntryPosition);
        } else {
          updatedEntryPosition = currentEntryPosition;
        }

        nextItem = getIntValue(updatedEntryPosition);
        if (nextItem > 0 && nextItem < entryPosition) {
          //update reference to the first item of linked list
          setIntValue(updatedEntryPosition, nextItem + entrySize);

          updateAllLinkedListReferences(nextItem, entryPosition, entrySize);
        }

        currentPositionOffset += OIntegerSerializer.INT_SIZE;
      }

    }
  }

  boolean remove(final int entryIndex, final ORID value) {
    assert isLeaf;

    final int entryPosition = getIntValue(POSITIONS_ARRAY_OFFSET + entryIndex * OIntegerSerializer.INT_SIZE);

    int position = entryPosition;
    int nextItem = getIntValue(position);
    position += OIntegerSerializer.INT_SIZE;

    final int keySize;
    if (encryption == null) {
      keySize = getObjectSizeInDirectMemory(keySerializer, position);
    } else {
      final int encryptedSize = getIntValue(position);
      keySize = encryptedSize + OIntegerSerializer.INT_SIZE;
    }

    position += keySize;

    //only single element in list
    if (nextItem == -1) {
      final int clusterId = getShortValue(position);
      if (clusterId != value.getClusterId()) {
        return false;
      }

      position += OShortSerializer.SHORT_SIZE;

      final long clusterPosition = getLongValue(position);
      if (clusterPosition == value.getClusterPosition()) {
        removeMainEntry(entryIndex, entryPosition, keySize);
        return true;
      }
    } else {
      int clusterId = getShortValue(position);
      long clusterPosition = getLongValue(position + OShortSerializer.SHORT_SIZE);

      if (clusterId == value.getClusterId() && clusterPosition == value.getClusterPosition()) {
        final int nextNextItem = getIntValue(nextItem);
        final byte[] nextValue = getBinaryValue(nextItem + OIntegerSerializer.INT_SIZE, RID_SIZE);

        setIntValue(entryPosition, nextNextItem);
        setBinaryValue(entryPosition + OIntegerSerializer.INT_SIZE + keySize, nextValue);

        final int freePointer = getIntValue(FREE_POINTER_OFFSET);
        setIntValue(FREE_POINTER_OFFSET, freePointer + OIntegerSerializer.INT_SIZE + RID_SIZE);

        if (nextItem > freePointer) {
          moveData(freePointer, freePointer + LINKED_LIST_ITEM_SIZE, nextItem - freePointer);

          final int size = getIntValue(SIZE_OFFSET);
          int currentPositionOffset = POSITIONS_ARRAY_OFFSET;

          for (int i = 0; i < size; i++) {
            final int currentEntryPosition = getIntValue(currentPositionOffset);
            final int updatedEntryPosition;

            if (currentEntryPosition < nextItem) {
              updatedEntryPosition = currentEntryPosition + LINKED_LIST_ITEM_SIZE;
              setIntValue(currentPositionOffset, updatedEntryPosition);
            } else {
              updatedEntryPosition = currentEntryPosition;
            }

            final int currentNextItem = getIntValue(updatedEntryPosition);
            if (currentNextItem > 0 && currentNextItem < nextItem) {
              //update reference to the first item of linked list
              setIntValue(updatedEntryPosition, currentNextItem + LINKED_LIST_ITEM_SIZE);

              updateAllLinkedListReferences(currentNextItem, nextItem, LINKED_LIST_ITEM_SIZE);
            }

            currentPositionOffset += OIntegerSerializer.INT_SIZE;
          }
        }

        return true;
      } else {
        int prevItem = entryPosition;

        while (nextItem > 0) {
          final int nextNextItem = getIntValue(nextItem);
          clusterId = getShortValue(nextItem + OIntegerSerializer.INT_SIZE);
          clusterPosition = getLongValue(nextItem + OIntegerSerializer.INT_SIZE + OShortSerializer.SHORT_SIZE);

          if (clusterId == value.getClusterId() && clusterPosition == value.getClusterPosition()) {
            setIntValue(prevItem, nextNextItem);

            final int freePointer = getIntValue(FREE_POINTER_OFFSET);
            setIntValue(FREE_POINTER_OFFSET, freePointer + LINKED_LIST_ITEM_SIZE);

            if (nextItem > freePointer) {
              moveData(freePointer, freePointer + LINKED_LIST_ITEM_SIZE, nextItem - freePointer);

              final int size = getIntValue(SIZE_OFFSET);
              int currentPositionOffset = POSITIONS_ARRAY_OFFSET;

              for (int i = 0; i < size; i++) {
                final int currentEntryPosition = getIntValue(currentPositionOffset);
                final int updatedEntryPosition;

                if (currentEntryPosition < nextItem) {
                  updatedEntryPosition = currentEntryPosition + LINKED_LIST_ITEM_SIZE;
                  setIntValue(currentPositionOffset, updatedEntryPosition);
                } else {
                  updatedEntryPosition = currentEntryPosition;
                }

                final int currentNextItem = getIntValue(updatedEntryPosition);
                if (currentNextItem > 0 && currentNextItem < nextItem) {
                  //update reference to the first item of linked list
                  setIntValue(updatedEntryPosition, currentNextItem + LINKED_LIST_ITEM_SIZE);

                  updateAllLinkedListReferences(currentNextItem, nextItem, LINKED_LIST_ITEM_SIZE);
                }

                currentPositionOffset += OIntegerSerializer.INT_SIZE;
              }
            }

            return true;
          }

          prevItem = nextItem;
          nextItem = nextNextItem;
        }
      }
    }

    return false;
  }

  private void updateAllLinkedListReferences(int firstItem, int boundary, int diffSize) {
    int currentItem = firstItem + diffSize;

    while (true) {
      final int nextItem = getIntValue(currentItem);

      if (nextItem > 0 && nextItem < boundary) {
        setIntValue(currentItem, nextItem + diffSize);
        currentItem = nextItem + diffSize;
      } else {
        return;
      }
    }
  }

  private int[] updateAllLinkedListReferences(int firstItem, int boundary, int currentDiffSize, int diffSize) {
    int currentItem = firstItem + currentDiffSize;

    while (true) {
      final int nextItem = getIntValue(currentItem);

      if (nextItem > 0 && nextItem < boundary) {
        setIntValue(currentItem, nextItem + diffSize);
        currentItem = nextItem + currentDiffSize;
      } else {
        return new int[] { currentItem, nextItem };
      }
    }
  }

  public int size() {
    return getIntValue(SIZE_OFFSET);
  }

  LeafEntry getLeafEntry(int entryIndex) {
    assert isLeaf;

    int entryPosition = getIntValue(entryIndex * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    byte[] key;
    int nextItem = getIntValue(entryPosition);
    entryPosition += OIntegerSerializer.INT_SIZE;

    if (encryption == null) {
      final int keySize = getObjectSizeInDirectMemory(keySerializer, entryPosition);
      key = getBinaryValue(entryPosition, keySize);

      entryPosition += keySize;
    } else {
      final int encryptionSize = getIntValue(entryPosition);
      key = getBinaryValue(entryPosition, encryptionSize + OIntegerSerializer.INT_SIZE);

      entryPosition += encryptionSize + OIntegerSerializer.INT_SIZE;
    }

    List<ORID> values = new ArrayList<>();

    int clusterId = getShortValue(entryPosition);
    entryPosition += OShortSerializer.SHORT_SIZE;

    long clusterPosition = getLongValue(entryPosition);

    values.add(new ORecordId(clusterId, clusterPosition));

    while (nextItem > 0) {
      int nextNextItem = getIntValue(nextItem);

      clusterId = getShortValue(nextItem + OIntegerSerializer.INT_SIZE);
      clusterPosition = getLongValue(nextItem + OShortSerializer.SHORT_SIZE + OIntegerSerializer.INT_SIZE);

      values.add(new ORecordId(clusterId, clusterPosition));

      nextItem = nextNextItem;
    }

    return new LeafEntry(key, values);
  }

  NonLeafEntry getNonLeafEntry(int entryIndex) {
    assert !isLeaf;

    int entryPosition = getIntValue(entryIndex * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    int leftChild = getIntValue(entryPosition);
    entryPosition += OIntegerSerializer.INT_SIZE;

    int rightChild = getIntValue(entryPosition);
    entryPosition += OIntegerSerializer.INT_SIZE;

    byte[] key;

    if (encryption == null) {
      final int keySize = getObjectSizeInDirectMemory(keySerializer, entryPosition);
      key = getBinaryValue(entryPosition, keySize);
    } else {
      final int encryptionSize = getIntValue(entryPosition);
      key = getBinaryValue(entryPosition, encryptionSize + OIntegerSerializer.INT_SIZE);
    }

    return new NonLeafEntry(key, leftChild, rightChild);
  }

  int getLeft(int entryIndex) {
    assert !isLeaf;

    final int entryPosition = getIntValue(entryIndex * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    return getIntValue(entryPosition);
  }

  int getRight(int entryIndex) {
    assert !isLeaf;

    final int entryPosition = getIntValue(entryIndex * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    return getIntValue(entryPosition + OIntegerSerializer.INT_SIZE);
  }

  /**
   * Obtains the value stored under the given entry index in this bucket.
   *
   * @param entryIndex the value entry index.
   *
   * @return the obtained value.
   */
  List<ORID> getValues(int entryIndex) {
    assert isLeaf;

    int entryPosition = getIntValue(entryIndex * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);
    int nextItem = getIntValue(entryPosition);
    entryPosition += OIntegerSerializer.INT_SIZE;

    // skip key
    if (encryption == null) {
      entryPosition += getObjectSizeInDirectMemory(keySerializer, entryPosition);
    } else {
      final int encryptedSize = getIntValue(entryPosition);
      entryPosition += OIntegerSerializer.INT_SIZE + encryptedSize;
    }

    int clusterId = getShortValue(entryPosition);
    long clusterPosition = getLongValue(entryPosition + OShortSerializer.SHORT_SIZE);

    final List<ORID> results = new ArrayList<>();
    results.add(new ORecordId(clusterId, clusterPosition));

    while (nextItem > 0) {
      final int nextNextItem = getIntValue(nextItem);

      clusterId = getShortValue(nextItem + OIntegerSerializer.INT_SIZE);
      clusterPosition = getLongValue(nextItem + OIntegerSerializer.INT_SIZE + OShortSerializer.SHORT_SIZE);

      results.add(new ORecordId(clusterId, clusterPosition));

      nextItem = nextNextItem;
    }

    return results;
  }

  public K getKey(int index) {
    int entryPosition = getIntValue(index * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    if (!isLeaf) {
      entryPosition += 2 * OIntegerSerializer.INT_SIZE;
    } else {
      entryPosition += OIntegerSerializer.INT_SIZE;
    }

    if (encryption == null) {
      return deserializeFromDirectMemory(keySerializer, entryPosition);
    } else {
      final int encryptedSize = getIntValue(entryPosition);
      entryPosition += OIntegerSerializer.INT_SIZE;

      final byte[] encryptedKey = getBinaryValue(entryPosition, encryptedSize);
      final byte[] serializedKey = encryption.decrypt(encryptedKey);
      return keySerializer.deserializeNativeObject(serializedKey, 0);
    }
  }

  byte[] getRawKey(int index) {
    int entryPosition = getIntValue(index * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);

    if (!isLeaf) {
      entryPosition += 2 * OIntegerSerializer.INT_SIZE;
    } else {
      entryPosition += OIntegerSerializer.INT_SIZE;
    }

    if (encryption == null) {
      final int keySize = getObjectSizeInDirectMemory(keySerializer, entryPosition);
      return getBinaryValue(entryPosition, keySize);
    } else {
      final int encryptedSize = getIntValue(entryPosition);
      return getBinaryValue(entryPosition, encryptedSize + OIntegerSerializer.INT_SIZE);
    }
  }

  public boolean isLeaf() {
    return isLeaf;
  }

  public void addAll(final List<Entry> entries) {
    if (!isLeaf) {
      for (int i = 0; i < entries.size(); i++) {
        final NonLeafEntry entry = (NonLeafEntry) entries.get(i);
        addNonLeafEntry(i, entry.key, entry.leftChild, entry.rightChild, false);
      }
    } else {
      for (int i = 0; i < entries.size(); i++) {
        final LeafEntry entry = (LeafEntry) entries.get(i);
        final byte[] key = entry.key;
        final List<ORID> values = entry.values;

        addNewLeafEntry(i, key, values.get(0));

        for (int n = 1; n < values.size(); n++) {
          appendNewLeafEntry(i, values.get(n));
        }
      }
    }

    setIntValue(SIZE_OFFSET, entries.size());
  }

  public void shrink(final int newSize) {
    if (isLeaf) {
      final List<LeafEntry> entries = new ArrayList<>(newSize);

      for (int i = 0; i < newSize; i++) {
        entries.add(getLeafEntry(i));
      }

      setIntValue(FREE_POINTER_OFFSET, MAX_PAGE_SIZE_BYTES);

      int index = 0;
      for (final LeafEntry entry : entries) {
        final byte[] key = entry.key;
        final List<ORID> values = entry.values;

        addNewLeafEntry(index, key, values.get(0));

        for (int n = 1; n < values.size(); n++) {
          appendNewLeafEntry(index, values.get(n));
        }
        index++;
      }

      setIntValue(SIZE_OFFSET, newSize);
    } else {
      final List<NonLeafEntry> entries = new ArrayList<>(newSize);

      for (int i = 0; i < newSize; i++) {
        entries.add(getNonLeafEntry(i));
      }

      setIntValue(FREE_POINTER_OFFSET, MAX_PAGE_SIZE_BYTES);

      int index = 0;
      for (final NonLeafEntry entry : entries) {
        addNonLeafEntry(index, entry.key, entry.leftChild, entry.rightChild, false);
        index++;
      }

      setIntValue(SIZE_OFFSET, newSize);
    }
  }

  void halfSingleEntry() {
    assert size() == 1;

    final int entryPosition = getIntValue(POSITIONS_ARRAY_OFFSET);
    final List<Integer> items = new ArrayList<>();
    items.add(entryPosition);

    int nextItem = entryPosition;
    while (true) {
      final int nextNextItem = getIntValue(nextItem);
      if (nextNextItem != -1) {
        items.add(nextNextItem);
      } else {
        break;
      }

      nextItem = nextNextItem;
    }

    final int size = items.size();
    final int halfIndex = size / 2;
    final List<Integer> itemsToRemove = items.subList(1, halfIndex + 1);

    final int lastItemPos = items.get(halfIndex);

    final int nextFirsItem = getIntValue(lastItemPos);
    final byte[] firstRid = getBinaryValue(lastItemPos + OIntegerSerializer.INT_SIZE, RID_SIZE);

    int freePointer = getIntValue(FREE_POINTER_OFFSET);

    for (int itemPos : itemsToRemove) {
      if (itemPos > freePointer) {
        moveData(freePointer, freePointer + OIntegerSerializer.INT_SIZE + RID_SIZE, nextItem - freePointer);
      }

      freePointer += OIntegerSerializer.INT_SIZE + RID_SIZE;
    }

    setIntValue(FREE_POINTER_OFFSET, freePointer);

    setIntValue(entryPosition, nextFirsItem);

    final int keySize;
    if (encryption == null) {
      keySize = getObjectSizeInDirectMemory(keySerializer, entryPosition + OIntegerSerializer.INT_SIZE);
    } else {
      final int encryptedSize = getIntValue(entryPosition + OIntegerSerializer.INT_SIZE);
      keySize = OIntegerSerializer.INT_SIZE + encryptedSize;
    }

    setBinaryValue(entryPosition + OIntegerSerializer.INT_SIZE + keySize, firstRid);
  }

  boolean addNewLeafEntry(final int index, final byte[] serializedKey, final ORID value) {
    assert isLeaf;

    final int entrySize = serializedKey.length + RID_SIZE + OIntegerSerializer.INT_SIZE; //next item pointer at the begging of entry
    final int size = getIntValue(SIZE_OFFSET);

    int freePointer = getIntValue(FREE_POINTER_OFFSET);
    if (freePointer - entrySize < (size + 1) * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET) {
      return false;
    }

    if (index <= size - 1) {
      moveData(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE,
          POSITIONS_ARRAY_OFFSET + (index + 1) * OIntegerSerializer.INT_SIZE, (size - index) * OIntegerSerializer.INT_SIZE);
    }

    freePointer -= entrySize;

    setIntValue(FREE_POINTER_OFFSET, freePointer);
    setIntValue(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE, freePointer);
    setIntValue(SIZE_OFFSET, size + 1);

    freePointer += setIntValue(freePointer, -1); //next item pointer
    freePointer += setBinaryValue(freePointer, serializedKey);//key
    freePointer += setShortValue(freePointer, (short) value.getClusterId());//rid
    setLongValue(freePointer, value.getClusterPosition());

    return true;
  }

  boolean appendNewLeafEntry(final int index, final ORID value) {
    assert isLeaf;

    final int itemSize = OIntegerSerializer.INT_SIZE + RID_SIZE;//next item pointer + RID
    int freePointer = getIntValue(FREE_POINTER_OFFSET);

    final int size = getIntValue(SIZE_OFFSET);

    if (freePointer - itemSize < size * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET + itemSize) {
      return false;
    }

    freePointer -= itemSize;
    setIntValue(FREE_POINTER_OFFSET, freePointer);

    final int entryPosition = getIntValue(index * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET);
    final int nextItem = getIntValue(entryPosition);

    setIntValue(entryPosition, freePointer);//update list header

    freePointer += setIntValue(freePointer, nextItem);//next item pointer
    freePointer += setShortValue(freePointer, (short) value.getClusterId());//rid
    setLongValue(freePointer, value.getClusterPosition());

    return true;
  }

  boolean addNonLeafEntry(int index, byte[] serializedKey, int leftChild, int rightChild, boolean updateNeighbors) {
    assert !isLeaf;

    int entrySize = serializedKey.length + 2 * OIntegerSerializer.INT_SIZE;

    int size = size();
    int freePointer = getIntValue(FREE_POINTER_OFFSET);
    if (freePointer - entrySize < (size + 1) * OIntegerSerializer.INT_SIZE + POSITIONS_ARRAY_OFFSET) {
      return false;
    }

    if (index <= size - 1) {
      moveData(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE,
          POSITIONS_ARRAY_OFFSET + (index + 1) * OIntegerSerializer.INT_SIZE, (size - index) * OIntegerSerializer.INT_SIZE);
    }

    freePointer -= entrySize;

    setIntValue(FREE_POINTER_OFFSET, freePointer);
    setIntValue(POSITIONS_ARRAY_OFFSET + index * OIntegerSerializer.INT_SIZE, freePointer);
    setIntValue(SIZE_OFFSET, size + 1);

    freePointer += setIntValue(freePointer, leftChild);
    freePointer += setIntValue(freePointer, rightChild);

    setBinaryValue(freePointer, serializedKey);

    size++;

    if (updateNeighbors && size > 1) {
      if (index < size - 1) {
        final int nextEntryPosition = getIntValue(POSITIONS_ARRAY_OFFSET + (index + 1) * OIntegerSerializer.INT_SIZE);
        setIntValue(nextEntryPosition, rightChild);
      }

      if (index > 0) {
        final int prevEntryPosition = getIntValue(POSITIONS_ARRAY_OFFSET + (index - 1) * OIntegerSerializer.INT_SIZE);
        setIntValue(prevEntryPosition + OIntegerSerializer.INT_SIZE, leftChild);
      }
    }

    return true;
  }

  void setLeftSibling(long pageIndex) {
    setLongValue(LEFT_SIBLING_OFFSET, pageIndex);
  }

  public long getLeftSibling() {
    return getLongValue(LEFT_SIBLING_OFFSET);
  }

  void setRightSibling(long pageIndex) {
    setLongValue(RIGHT_SIBLING_OFFSET, pageIndex);
  }

  public long getRightSibling() {
    return getLongValue(RIGHT_SIBLING_OFFSET);
  }

  static class Entry {
    final byte[] key;

    public Entry(byte[] key) {
      this.key = key;
    }
  }

  static final class LeafEntry extends Entry {
    final List<ORID> values;

    LeafEntry(byte[] key, List<ORID> values) {
      super(key);
      this.values = values;
    }
  }

  static final class NonLeafEntry extends Entry {
    final int leftChild;
    final int rightChild;

    NonLeafEntry(byte[] key, int leftChild, int rightChild) {
      super(key);

      this.leftChild = leftChild;
      this.rightChild = rightChild;
    }
  }
}
