package com.orientechnologies.lucene.index;

import com.orientechnologies.lucene.collections.OLuceneIndexCursor;
import com.orientechnologies.lucene.collections.OLuceneResultSet;
import com.orientechnologies.orient.core.index.OIndexCursor;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Created by Enrico Risa on 07/08/2018.
 */
public class OLuceneIndexNotUniqueImpl extends OLuceneFullTextIndex {

  public OLuceneIndexNotUniqueImpl(String name, String typeId, String algorithm, int version, OAbstractPaginatedStorage storage,
      String valueContainerAlgorithm, ODocument metadata) {
    super(name, typeId, algorithm, version, storage, valueContainerAlgorithm, metadata);
  }

  @Override
  public boolean canBeUsedInEqualityOperators() {
    return true;
  }

  @Override
  public OIndexCursor iterateEntries(Collection<?> keys, boolean ascSortOrder) {

    List<String> values = keys.stream().map((c -> c.toString())).collect(Collectors.toList());

    List<String> fields = getDefinition().getFields();
    String query = IntStream.range(0, values.size()).mapToObj((i) -> String.format("%s:\"%s\"", fields.get(i), values.get(i)))
        .collect(Collectors.joining(" AND "));

    OLuceneResultSet identifiables = (OLuceneResultSet) get(query);

    return new OLuceneIndexCursor(identifiables, query);

  }
}
