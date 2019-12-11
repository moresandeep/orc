/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.orc.tools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.tools.convert.ConvertTool;
import org.apache.orc.tools.convert.JsonReader;
import org.apache.orc.tools.json.JsonSchemaFinder;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONWriter;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.zip.GZIPInputStream;

public class TestExampleFiles {

  final String HADOOP_PATH = "../../../../";
  final String PATH = "../../";
  final String EXAMPLE_DIR = "examples/";
  final String EXPECTED_DIR = "expected/";
  final String INPUT_FILE_EXTENSION = ".orc";
  final String EXPECTED_FILE_EXTENSION = ".jsn.gz";

  Path workDir = new Path(System.getProperty("test.tmp.dir"));
  Configuration conf;
  FileSystem fs;
  //Path testFilePath;

  @Before
  public void init() throws Exception {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf);
    fs.setWorkingDirectory(workDir);
  }

  private void compareRowBatch(final String testName)
      throws IOException, JSONException {

    final Path testFilePath = new Path(HADOOP_PATH+EXAMPLE_DIR+testName+INPUT_FILE_EXTENSION);
    final String expected = PATH+EXAMPLE_DIR+EXPECTED_DIR+testName+EXPECTED_FILE_EXTENSION;

    final Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));

    try (final RecordReader rows = reader.rows();
        final FileInputStream fis = new FileInputStream(expected);
        final GZIPInputStream gis = new GZIPInputStream(fis);
        final InputStreamReader isr = new InputStreamReader(gis);
        final BufferedReader in = new BufferedReader(isr)
    ) {

      String line = null;

      final TypeDescription schema = reader.getSchema();
      final VectorizedRowBatch batch = schema.createRowBatch();
      System.out.println( "Batch Size: "+ batch.size);

      while (rows.nextBatch(batch)) {
        for(int r=0; r < batch.size; ++r) {
          final StringWriter out = new StringWriter();
          final JSONWriter writer = new JSONWriter(out);
          PrintData.printRow(writer, batch, schema, r);
          // expected
          line = in.readLine();
          /* condition the output to the format java writer expects, this does not modify json structure */
          String conditioned = StringUtils.replaceEach(StringUtils.deleteWhitespace(line), new String[]{"key", "value"}, new String[]{"_key", "_value"}) ;
          System.out.println("-------------------------------------------------");
          System.out.println("Expected:");
          System.out.println(conditioned);
          System.out.println("Actual:");
          System.out.println(out.toString());
          System.out.println("-------------------------------------------------");
          assertEquals(conditioned, out.toString());
        }
      }

    }

  }

  public void printExpected(final String fileName) throws IOException {
    final String expected = PATH+EXAMPLE_DIR+EXPECTED_DIR+fileName+EXPECTED_FILE_EXTENSION;

    try (final FileInputStream fis = new FileInputStream(expected);
        final GZIPInputStream gis = new GZIPInputStream(fis);
        final InputStreamReader isr = new InputStreamReader(gis);
        final BufferedReader in = new BufferedReader(isr)
    ) {
      System.out.println("Expected:");
      String line = null;
      while( (line = in.readLine()) != null) {
        System.out.println("-------------------------------------------------");
        System.out.println(StringUtils.deleteWhitespace(line));
        System.out.println("-------------------------------------------------");
      }
    }

  }

  public void printActual(final String fileName)
      throws IOException, JSONException {
    final Path testFilePath = new Path(HADOOP_PATH+EXAMPLE_DIR+fileName+INPUT_FILE_EXTENSION);

    final Reader reader = OrcFile.createReader(testFilePath,
        OrcFile.readerOptions(conf).filesystem(fs));

    try (final RecordReader rows = reader.rows()) {

      final TypeDescription schema = reader.getSchema();
      final VectorizedRowBatch batch = schema.createRowBatch();
      System.out.println( "Batch Size: "+ batch.size);
      System.out.println("Actual:");
      while (rows.nextBatch(batch)) {
        for(int r=0; r < batch.size; ++r) {
          final StringWriter out = new StringWriter();
          JSONWriter writer = new JSONWriter(out);
          PrintData.printRow(writer, batch, schema, r);
          System.out.println("-------------------------------------------------");
          System.out.println(out.toString());
          System.out.println("-------------------------------------------------");
        }
      }

    }
  }

  static void printBinary( BytesColumnVector vector,
      int row) throws JSONException {
    int offset = vector.start[row];
    for(int i=0; i < vector.length[row]; ++i) {
      System.out.println(vector.vector[row][offset + i]);
      //writer.value(0xff & (int) vector.vector[row][offset + i]);
    }
  }

  public void compareRows(final String testName)
      throws IOException, JSONException, ParseException {

    final VectorizedRowBatch expectedBatch;
    final VectorizedRowBatch testBatch;

    final Path testFilePath = new Path(HADOOP_PATH+EXAMPLE_DIR+testName+INPUT_FILE_EXTENSION);
    final Path expectedFilePath = new Path(HADOOP_PATH+EXAMPLE_DIR+EXPECTED_DIR+testName+EXPECTED_FILE_EXTENSION);

    final Reader readerTestFile = OrcFile.createReader(testFilePath, OrcFile.readerOptions(conf).filesystem(fs));
    final TypeDescription schema = readerTestFile.getSchema();
    final RecordReader testRows = readerTestFile.rows();
    testBatch = schema.createRowBatch();
    testRows.nextBatch(testBatch);

    final FSDataInputStream expectedFis = fs.open(expectedFilePath);
    final java.io.Reader r = new InputStreamReader(new GZIPInputStream(expectedFis), StandardCharsets.UTF_8);

    // FIXME look at the length issue
    /* JsonStreamParser has issues working with Gziped reader */
    String result = IOUtils.toString(r);
    System.out.println("Result: "+result);

    //JsonSchemaFinder schemaFinder = new JsonSchemaFinder();
    ///schemaFinder.addFile(r);
    //final TypeDescription expectedSchema = schemaFinder.getSchema();
    final TypeDescription expectedSchema = schema;
    //final TypeDescription expectedSchema = ct.getSchema();

    final RecordReader readerExpected = new JsonReader(new StringReader(result), expectedFis, fs.getFileStatus(testFilePath).getLen(), expectedSchema);

    System.out.println("Expected readerExpected toString(): "+readerExpected.toString());

    System.out.println("Expected Schema: "+expectedSchema.toString());
    System.out.println("Test Schema: "+schema.toString());

    //assertTrue("Schemas don't match", expectedSchema.toString().equalsIgnoreCase(schema.toString()) );

    expectedBatch = expectedSchema.createRowBatch();

    // java.io.EOFException: End of input at line 1 column 1
    while(readerExpected.nextBatch(expectedBatch)) {
      System.out.println("Found data in expected");
    }

    for(int i=0; i < expectedBatch.size; ++i) {
      System.out.println("Expected: "+expectedBatch.cols.length);
    }

    for(int i=0; i < testBatch.size; ++i) {
      System.out.println("Test: "+testBatch.cols.length);
    }

    /*
    while(readerExpected.nextBatch(expectedBatch)) {

    }
     */

    System.out.println("Expected rows: "+readerExpected.getRowNumber());
    System.out.println("Actual rows: "+readerTestFile.getNumberOfRows());

  /*
    try (final RecordReader rows = reader.rows()) {

      }
    */

  }

  /* Main test */
  @Test
  public void testFiles()
      throws IOException, JSONException, ParseException {
    String fileName = "TestOrcFile.test1";
    //printExpected(fileName);
    //printActual(fileName);
    //compareRowBatch(fileName);
    compareRows(fileName);
  }


  @Test
  public void printData() throws IOException, JSONException {

    String FILE_NAME = "TestOrcFile.test1.jsn.gz";

    String expected = PATH+EXAMPLE_DIR+EXPECTED_DIR+FILE_NAME;

    try (FileInputStream fis = new FileInputStream(expected);
        GZIPInputStream gis = new GZIPInputStream(fis);
        InputStreamReader reader = new InputStreamReader(gis);
        BufferedReader in = new BufferedReader(reader)) {

      String line;

      while( (line = in.readLine()) != null) {
        System.out.println(line);
        System.out.println("-------------------------------------------");
      }

    }
  }


}
