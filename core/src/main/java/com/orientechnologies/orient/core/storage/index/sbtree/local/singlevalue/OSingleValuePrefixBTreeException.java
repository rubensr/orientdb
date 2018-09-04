package com.orientechnologies.orient.core.storage.index.sbtree.local.singlevalue;

import com.orientechnologies.orient.core.exception.ODurableComponentException;
import com.orientechnologies.orient.core.storage.index.sbtree.local.OSBTreeException;

public class OSingleValuePrefixBTreeException extends ODurableComponentException {

  public OSingleValuePrefixBTreeException(OSBTreeException exception) {
    super(exception);
  }

  public OSingleValuePrefixBTreeException(String message, OSingleValuePrefixBTree component) {
    super(message, component);
  }
}

