/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.shell.command;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.file.FileSystem;
import alluxio.exception.AlluxioException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.io.EOFException;
import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Loads a file or directory in Alluxio space, makes it resident in memory.
 */
@ThreadSafe
public final class ReadTestCommand extends WithWildCardPathCommand {

  /**
   * @param fs the filesystem of Alluxio
   */
  public ReadTestCommand(FileSystem fs) {
    super(fs);
  }

  @Override
  public String getCommandName() {
    return "readTest";
  }

  @Override
  public Options getOptions() {
    return new Options()
        .addOption(Option.builder()
            .longOpt("position")
            .required(false)
            .hasArg(true)
            .desc("the read position.")
            .build())
        .addOption(Option.builder()
            .longOpt("len")
            .required(false)
            .hasArg(true)
            .desc("the len to read.")
            .build())
        .addOption(Option.builder()
            .longOpt("bufLen")
            .required(false)
            .hasArg(true)
            .desc("the buffer len.")
            .build())
        .addOption(Option.builder("A")
            .required(false)
            .hasArg(false)
            .desc("ReadAll.")
            .build())
        .addOption(Option.builder("C")
            .required(false)
            .hasArg(false)
            .desc("Close.")
            .build());
  }

  @Override
  protected void runCommand(AlluxioURI path, CommandLine cl) throws AlluxioException, IOException {
    long position = 0;
    long len = 0;
    int bufLen = 8 * Constants.MB;
    if (cl.hasOption("position")) {
      position = Long.parseLong(cl.getOptionValue("position"));
    }
    if (cl.hasOption("len")) {
      len = Long.parseLong(cl.getOptionValue("len"));
    }
    if (cl.hasOption("bufLen")) {
      bufLen = Integer.parseInt(cl.getOptionValue("bufLen")) * Constants.MB;
    }
    System.out.println("position = " + position);
    System.out.println("len = " + len);
    System.out.println("bufLen = " + bufLen);
    Path fsPath = new Path(path.toString());
    Configuration conf = new Configuration();
    org.apache.hadoop.fs.FileSystem fileSystem = fsPath.getFileSystem(conf);
    FSDataInputStream inputStream = null;
    try {
      inputStream = fileSystem.open(fsPath);
      FileStatus fileStatus = fileSystem.getFileStatus(fsPath);
      byte[] buffer = new byte[bufLen];
      long readLen = len;
      if (!cl.hasOption("A")) {
        if (readLen == 0) {
          readLen = ((fileStatus.getLen() - position) > buffer.length
                  ? buffer.length : fileStatus.getLen() - position);
        }
        System.out.println("readLen = " + readLen);
      } else {
        if (readLen == 0) {
          readLen = (fileStatus.getLen() - position);
        }
        while (position < readLen) {
          long curReadLen =
              (readLen - position) > buffer.length ? buffer.length : (readLen - position);
          inputStream.readFully(position, buffer, 0, (int) curReadLen);
          position += curReadLen;
        }
        System.out.println("current position = " + position);
      }
    } catch (EOFException e) {
      e.printStackTrace();
    } finally {
      if (cl.hasOption("C") && inputStream != null) {
        inputStream.close();
      }
    }
  }

  @Override
  public String getUsage() {
    return "readTest [--position=POST] <path>";
  }

  @Override
  public String getDescription() {
    return "test read files in Alluxio space, give a test report.";
  }

}
