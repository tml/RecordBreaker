/*
 * Copyright (c) 2011, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */
package com.cloudera.learnavro.test;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.OutputStreamWriter;
import java.io.FileOutputStream;

import java.util.Random;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.reflect.ReflectData;
import org.codehaus.jackson.JsonGenerator;

import com.cloudera.recordbreaker.schemadict.TestRecord;
import com.cloudera.recordbreaker.schemadict.SchemaSuggest;

/*********************************************************************
 * This class generates a number of test data files for the schema inference program.
 * It makes data for five different genres:
 * 1) A Web crawl
 * 2) An access log
 * 3) A file listing
 * 4) Sensor data
 * 5) Purchase transactions
 * 
 * We also attempt to generate statistically-plausible data for each
 *
 * @author mjc
 ***********************************************************************/
public class GenerateTestAvro {
  static long DAY_IN_MILLIS = 1000 * 60 * 60 * 24;
  static long WEEK_IN_MILLIS = 7 * DAY_IN_MILLIS;
  static int NUM_ALPHA = 32;
  static int CAPITAL_A = 65;
  static int LOWER_A = 97;
  static int RESULT_CODES[] = {200, 401, 501, 301, 403};
  static double RESULT_CODE_DIST[] = {.8, .1, .02, .02, .06};

  static Random r = new Random(3333);
  public GenerateTestAvro() {
  }

  
  /**
   * Main method for building all the test data files.
   */
  public void generateData(File outDir, int numRecords) throws IOException, InstantiationException {
    // Create the target dir
    outDir = outDir.getCanonicalFile();
    if (outDir.exists()) {
      throw new IOException("Directory already exists: " + outDir);
    }
    outDir.mkdirs();

    //
    // Emit WebPage data.  Note the weird "Instantiator" business that appears as if it could be done
    // via Class.newInstance().  We can't do that here because newInstance() is incompatible with inner
    // classes.
    //
    Schema webCrawlSchema = ReflectData.get().getSchema(WebPage.class);
    emitSchema(new File(outDir, "webcrawl.schema"), webCrawlSchema);
    emitData(new File(outDir, "webcrawl.dat"), webCrawlSchema, numRecords, new Instantiator<WebPage>() {
        public WebPage create() {
          return new WebPage();
        }
      });

    //
    // Access log
    //
    Schema accessLogSchema = ReflectData.get().getSchema(AccessLog.class);
    emitSchema(new File(outDir, "accesslog.schema"), accessLogSchema);
    emitData(new File(outDir, "accesslog.dat"), accessLogSchema, numRecords, new Instantiator<AccessLog>() {
        public AccessLog create() {
          return new AccessLog();
        }
      });

    //
    // File listing
    //
    Schema fileListingSchema = ReflectData.get().getSchema(FileListing.class);
    emitSchema(new File(outDir, "filelisting.schema"), fileListingSchema);
    emitData(new File(outDir, "filelisting.dat"), fileListingSchema, numRecords, new Instantiator<FileListing>() {
        public FileListing create() {
          return new FileListing();
        }
      });

    //
    // Sensor data
    //
    Schema sensorDataSchema = ReflectData.get().getSchema(SensorData.class);
    emitSchema(new File(outDir, "sensordata.schema"), sensorDataSchema);
    emitData(new File(outDir, "sensordata.dat"), sensorDataSchema, numRecords, new Instantiator<SensorData>() {
        public SensorData create() {
          return new SensorData();
        }
      });

    //
    // Purchases
    //
    Schema purchaseSchema = ReflectData.get().getSchema(Purchase.class);
    emitSchema(new File(outDir, "purchase.schema"), purchaseSchema);
    emitData(new File(outDir, "purchase.dat"), purchaseSchema, numRecords, new Instantiator<Purchase>() {
        public Purchase create() {
          return new Purchase();
        }
      });
  }

  ///////////////////////////
  // The sample classes
  ///////////////////////////
  /**
   * Data type #1: WebPage
   */
  public class WebPage  {
    String url;
    long dateCrawled;
    double rank;
    int lastResultCode;
    int failedAttempts;
    long nextCrawl;
    String content;
    public WebPage() {
      this.url = "http://" + generateRandomString(10, 100);
      this.dateCrawled = System.currentTimeMillis() - (Math.abs(r.nextLong()) % WEEK_IN_MILLIS);
      this.rank = r.nextDouble();
      this.lastResultCode = RESULT_CODES[chooseIndex(RESULT_CODE_DIST)];
      this.failedAttempts = r.nextInt(3);
      this.nextCrawl = dateCrawled + WEEK_IN_MILLIS;
      this.content = generateRandomString(1024, 10 * 1024);
    }
  }

  /**
   * Data type #2: access log.  (Taken from Pavlo, et al, SIGMOD 2009)
   */
  public class AccessLog  {
    String srcIP;
    String destURL;
    long visitDate;
    float adRevenue;
    String userAgent;
    String countryCode;
    String languageCode;
    String searchWord;
    int duration;
    public AccessLog() {
      this.srcIP = generateRandomString(12, 12);
      this.destURL = "http://" + generateRandomString(10, 100);
      this.visitDate = System.currentTimeMillis() - (Math.abs(r.nextLong()) % DAY_IN_MILLIS);
      this.adRevenue = Math.abs(r.nextFloat()) * 100;
      this.userAgent = generateRandomString(4, 10);
      this.countryCode = generateRandomString(2, 2);
      this.languageCode = generateRandomString(4, 4);
      this.searchWord = generateRandomString(4, 20);
      this.duration = r.nextInt(10000);
    }
  }

  /**
   * Data type #3: file listing
   */
  public class FileListing  {
    String permissions;
    String user;
    String group;
    int size;
    String month;
    int day;
    String time;
    String filename;

    public FileListing() {
      this.permissions = generateRandomString(10, 10);
      this.user = generateRandomString(2, 8);
      this.group = generateRandomString(5, 8);      
      this.size = r.nextInt(9086);
      this.month = generateRandomString(3, 3);
      this.day = r.nextInt(31);
      this.time = generateRandomString(5, 5);
      this.filename = generateRandomString(3, 20);
    }
  }

  /**
   * Data type #4: sensor data
   */
  public class SensorData  {
    double temp;
    double lumens;
    double pressure;
    long timestamp;
    int xpos;
    int ypos;
    int zpos;

    public SensorData() {
      this.temp = r.nextDouble() * 120;
      this.lumens = r.nextDouble() * 15000;
      this.pressure = r.nextDouble();
      this.timestamp = System.currentTimeMillis() + (Math.abs(r.nextLong()) % DAY_IN_MILLIS);
      this.xpos = r.nextInt(1000);
      this.ypos = r.nextInt(1000);
      this.zpos = r.nextInt(1000);
    }
  }

  /**
   * Data type #5: purchases
   */
  public class Purchase  {
    long productCode;
    String productDesc;
    double price;
    long timestamp;
    int quantity;

    public Purchase() {
      this.productCode = Math.abs(r.nextLong());
      this.productDesc = generateRandomString(15, 25);
      this.price = r.nextDouble() * 10000;
      this.timestamp = System.currentTimeMillis() + (Math.abs(r.nextLong()) % DAY_IN_MILLIS);
      this.quantity = r.nextInt(10);
    }
  }


  /////////////////////////////
  // Utility file-handling
  //////////////////////////////
  /**
   */
  void emitSchema(File outSchema, Schema schema) throws IOException {
    OutputStreamWriter out = new OutputStreamWriter(new BufferedOutputStream(new FileOutputStream(outSchema)));
    try {
      out.write(schema.toString(true));
    } finally {
      out.close();
    }         
  }

  /**
   */
  void emitData(File outData, Schema schema, int numRecords, Instantiator inster) throws IOException, InstantiationException {
    DatumWriter dout = new ReflectDatumWriter(schema);

    DataFileWriter out = new DataFileWriter(dout);
    out = out.create(schema, outData);
    try {
      for (int i = 0; i < numRecords; i++) {
        out.append(inster.create());
      }
      //encoder.flush();
    } finally {
      out.close();
    }
  }

  /////////////////////////////
  // Utility class-handling
  //////////////////////////////
  interface Instantiator<T> {
    public T create();
  }

  /////////////////////////////
  // Utility random-gen
  //////////////////////////////
  String generateRandomString(int minLen, int maxLen) {
    int target = minLen;
    if (maxLen - minLen > 0) {
      target += r.nextInt(maxLen-minLen);
    }

    StringBuffer buf = new StringBuffer();
    for (int i = 0; i < target; i++) {
      int rval = r.nextInt(NUM_ALPHA * 2);
      if (rval < NUM_ALPHA) {
        buf.append((char) (CAPITAL_A + rval));
      } else {
        buf.append((char) (LOWER_A + rval));
      }
    }
    return buf.toString();
  }

  /**
   * We require, but do not test, that the contents of distribution sums to 1.0
   */
  int chooseIndex(double[] distribution) {
    double target = r.nextDouble();
    for (int i = 0; i < distribution.length; i++) {
      target -= distribution[i];
      if (target <= 0) {
        return i;
      }
    }
    return distribution.length-1;
  }

  /**
   */
  public static void main(String argv[]) throws IOException, InstantiationException {
    CommandLine cmd = null;
    Options options = new Options();
    options.addOption("?", false, "Help for command-line");
    options.addOption("n", true, "# tuples to emit per file");

    try {
      CommandLineParser parser = new PosixParser();
      cmd = parser.parse(options, argv);
    } catch (ParseException pe) {
      HelpFormatter fmt = new HelpFormatter();
      fmt.printHelp("GenerateTestAvro", options, true);
      System.exit(-1);
    }

    if (cmd.hasOption("?")) {
      HelpFormatter fmt = new HelpFormatter();
      fmt.printHelp("GenerateTestAvro", options, true);
      System.exit(0);
    }

    int numToEmit = 100;
    if (cmd.hasOption("n")) {
      try {
        numToEmit = Integer.parseInt(cmd.getOptionValue("n"));
      } catch (NumberFormatException nfe) {
        nfe.printStackTrace();
      }
    }

    String[] argArray = cmd.getArgs();
    if (argArray.length == 0) {
      HelpFormatter fmt = new HelpFormatter();
      fmt.printHelp("GenerateTestAvro", options, true);
      System.exit(0);
    }
    File outputDir = new File(argArray[0]).getCanonicalFile();

    GenerateTestAvro gta = new GenerateTestAvro();
    gta.generateData(outputDir, numToEmit);
  }
}
