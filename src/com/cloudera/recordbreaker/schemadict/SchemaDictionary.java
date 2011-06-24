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
package com.cloudera.recordbreaker.schemadict;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;

import java.util.List;
import java.util.Random;
import java.util.ArrayList;

import org.apache.avro.Schema;

/******************************************
 * A SchemaDictionary holds a number of serialized SchemaDictionaryElt objects, each of
 * which holds some schema info, a SchemaStatisticalSummary, and a user comment.
 * 
 * A SchemaDictionary is meant to be the "clean schema reference" that helps users give
 * a name to novel schemas.
 *
 * @author mjc
 ******************************************/
public class SchemaDictionary {
  File dir;
  Random r = new Random();
  List<SchemaDictionaryEntry> dictElts = new ArrayList<SchemaDictionaryEntry>();

  /**
   * Load the schema dictionary from the given directory.
   */
  public SchemaDictionary(File dir) throws IOException {
    this.dir = dir.getCanonicalFile();
    if (! dir.exists()) {
      if (! dir.mkdirs()) {
        throw new IOException("Could not create: " + dir);
      }
    }

    File dictFiles[] = dir.listFiles(new FilenameFilter() {
      public boolean accept(File dir, String name) {
        return name.endsWith(SchemaDictionaryEntry.SUMMARY_ENDING);
      }
    });

    for (int i = 0; i < dictFiles.length; i++) {
      String name = dictFiles[i].getName();
      String fileRoot = name.substring(0, name.length() - SchemaDictionaryEntry.SUMMARY_ENDING.length());
      SchemaDictionaryEntry sde = new SchemaDictionaryEntry();
      sde.loadDictionaryEntry(dir, fileRoot);
      dictElts.add(sde);
    }
  }

  /**
   * Store a novel dictionary element (which is constructed with the original datafile and a user's comment).
   */
  public synchronized void addDictionaryElt(File avroFile, String infoText) throws IOException {
    SchemaDictionaryEntry entry = new SchemaDictionaryEntry(avroFile, infoText);
    dictElts.add(entry);

    String fileRoot = "" + Math.abs(r.nextInt());
    entry.saveDictionaryEntry(dir, fileRoot);
  }

  /**
   * Iterate through objects already in the directory.
   */
  public List<SchemaDictionaryEntry> contents() {
    return dictElts;
  }

  //////////////////////////////////////////
  // main()
  //////////////////////////////////////////
  public static void main(String argv[]) throws IOException {
    boolean shouldDump = false;
    boolean shouldAdd = false;
    File avroDataFile = null;
    String dictMessage = null;

    CommandLine cmd = null;
    Options options = new Options();
    options.addOption("?", false, "Help for command-line");
    options.addOption("d", false, "Dump contents of schema dictionary");
    options.addOption("a", true, "Add datafile to new schema dictionary element");
    options.addOption("m", true, "Add comment message as part of new schema dictionary element");

    try {
      CommandLineParser parser = new PosixParser();
      cmd = parser.parse(options, argv);
    } catch (ParseException e) {
      HelpFormatter fmt = new HelpFormatter();
      fmt.printHelp("SchemaDictionary", options, true);
      System.err.println("Required input: <schemadictionary>");
      System.exit(-1);
    }

    if (cmd.hasOption("?")) {
      HelpFormatter fmt = new HelpFormatter();
      fmt.printHelp("SchemaDictionary", options, true);
      System.err.println("Required input: <schemadictionary>");
      System.exit(0);
    }

    if (cmd.hasOption("d")) {
      shouldDump = true;
    }

    if (cmd.hasOption("a")) {
      avroDataFile = new File(cmd.getOptionValue("a")).getCanonicalFile();
    }
    if (cmd.hasOption("m")) {
      dictMessage = cmd.getOptionValue("m");
      if (cmd.hasOption("a")) {
        shouldAdd = true;
      }
    }
    if ((! shouldAdd) && (cmd.hasOption("a") || cmd.hasOption("m"))) {
      System.err.println("Must indicate -a AND -m to add new schema dictionary item");
      HelpFormatter fmt = new HelpFormatter();
      fmt.printHelp("SchemaDictionary", options, true);
      System.err.println("Required input: <schemadictionary>");
      System.exit(0);
    }

    String[] argArray = cmd.getArgs();
    if (argArray.length == 0) {
      System.err.println("No schema dictionary path provided.");
      HelpFormatter fmt = new HelpFormatter();
      fmt.printHelp("SchemaDictionary", options, true);
      System.err.println("Required input: <schemadictionary>");
      System.exit(0);
    }

    File dictionaryDir = new File(argArray[0]).getCanonicalFile();
    SchemaDictionary dict = new SchemaDictionary(dictionaryDir);

    if (shouldAdd) {
      dict.addDictionaryElt(avroDataFile, dictMessage);
    }

    if (shouldDump) {
      int counter = 1;
      for (SchemaDictionaryEntry entry: dict.contents()) {
        System.err.println("" + counter + ".  " + entry.getInfo());
        System.err.println(entry.getSchema());
        System.err.println();
        counter++;
      }
      int numItems = counter-1;
      System.err.println("Dictionary at " + dictionaryDir.getCanonicalPath() + " has " + numItems + " item(s).");
    }
  }
}