package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author Luigi Dell'Aquila
 */
public class ODropClassStatementExecutionTest {
  static ODatabaseDocument db;

  @BeforeClass public static void beforeClass() {
    db = new ODatabaseDocumentTx("memory:ODropClassStatementExecutionTest");
    db.create();
    OClass v = db.getMetadata().getSchema().getClass("V");
    if (v == null) {
      db.getMetadata().getSchema().createClass("V");
    }
  }

  @AfterClass public static void afterClass() {
    db.close();
  }

  @Test public void testPlain() {
    String className = "testPlain";
    OSchema schema = db.getMetadata().getSchema();
    schema.createClass(className);

    schema.reload();
    Assert.assertNotNull(schema.getClass(className));

    OTodoResultSet result = db.command("drop class " + className);
    Assert.assertTrue(result.hasNext());
    OResult next = result.next();
    Assert.assertEquals("drop class", next.getProperty("operation"));
    Assert.assertFalse(result.hasNext());
    result.close();
    schema.reload();
    Assert.assertNull(schema.getClass(className));
  }

  @Test public void testUnsafe() {

    String className = "testUnsafe";
    OSchema schema = db.getMetadata().getSchema();
    OClass v = schema.getClass("V");
    schema.createClass(className, v);

    db.command("insert into " + className + " set foo = 'bar'");
    try {

      OTodoResultSet result = db.command("drop class " + className);
      Assert.fail();
    } catch (OCommandExecutionException ex1) {
    } catch (Exception ex2) {
      Assert.fail();
    }
    OTodoResultSet result = db.command("drop class " + className + " unsafe");
    Assert.assertTrue(result.hasNext());
    OResult next = result.next();
    Assert.assertEquals("drop class", next.getProperty("operation"));
    Assert.assertFalse(result.hasNext());
    result.close();
    schema.reload();
    Assert.assertNull(schema.getClass(className));
  }

  private void printExecutionPlan(String query, OTodoResultSet result) {
    if (query != null) {
      System.out.println(query);
    }
    result.getExecutionPlan().ifPresent(x -> System.out.println(x.prettyPrint(0, 3)));
    System.out.println();
  }

}
