package com.orientechnologies.orient.core.index.engine;

import com.orientechnologies.orient.core.id.ORID;

import java.util.Collection;

public interface OMultiValueIndexEngine extends OBaseIndexEngine {
  int VERSION = 1;

  boolean remove(Object key, ORID value);

  Collection<ORID> get(Object key);

  @Override
  default int getEngineVersion() {
    return VERSION;
  }
}
