package com.orientechnologies.orient.core.index.engine;

import com.orientechnologies.orient.core.id.ORID;

public interface OSingleValueIndexEngine extends OBaseIndexEngine {
  int VERSION = 1;

  boolean remove(Object key);

  ORID get(Object key);

  boolean validatedPut(Object key, ORID value, Validator<Object, ORID> validator);

  @Override
  default int getEngineVersion() {
    return VERSION;
  }
}
