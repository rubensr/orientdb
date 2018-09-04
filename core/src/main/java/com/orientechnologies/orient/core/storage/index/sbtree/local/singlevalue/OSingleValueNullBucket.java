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
package com.orientechnologies.orient.core.storage.index.sbtree.local.singlevalue;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OCompactedLinkSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;

import java.io.IOException;

/**
 * Bucket which is intended to save values stored in sbtree under <code>null</code> key. Bucket has following layout:
 * <ol>
 * <li>First byte is flag which indicates presence of value in bucket</li>
 * <li>Second byte indicates whether value is presented by link to the "bucket list" where actual value is stored or real value
 * passed be user.</li>
 * <li>The rest is serialized value whether link or passed in value.</li>
 * </ol>
 *
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 4/15/14
 */
public class OSingleValueNullBucket extends ODurablePage {
  public OSingleValueNullBucket(OCacheEntry cacheEntry, boolean isNew) {
    super(cacheEntry);

    if (isNew) {
      setByteValue(NEXT_FREE_POSITION, (byte) 0);
    }
  }

  public void setValue(ORID value) throws IOException {
    setByteValue(NEXT_FREE_POSITION, (byte) 1);

    final byte[] serializedValue = OCompactedLinkSerializer.INSTANCE.serializeNativeAsWhole(value);
    OCompactedLinkSerializer.INSTANCE.serializeNativeObject(value, serializedValue, 0);
    setBinaryValue(NEXT_FREE_POSITION + 1, serializedValue);
  }

  public ORID getValue() {
    if (getByteValue(NEXT_FREE_POSITION) == 0) {
      return null;
    }

    return deserializeFromDirectMemory(OCompactedLinkSerializer.INSTANCE, NEXT_FREE_POSITION + 1).getIdentity();
  }

  public void removeValue() {
    setByteValue(NEXT_FREE_POSITION, (byte) 0);
  }
}
