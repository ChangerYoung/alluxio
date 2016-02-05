/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.shell.command;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

import com.google.common.base.Joiner;

import org.apache.commons.cli.CommandLine;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.Configuration;
import alluxio.shell.AlluxioShellUtils;

/**
 * An abstract class for the commands that take exactly one path that could contain wildcard
 * characters.
 *
 * It will first do a glob against the input pattern then run the command for each expanded path.
 */
@ThreadSafe
public abstract class WithWildCardPathCommand extends AbstractShellCommand {

  protected WithWildCardPathCommand(Configuration conf, FileSystem fs) {
    super(conf, fs);
  }

  /**
   * Actually runs the command against one expanded path.
   *
   * @param path the expanded input path
   * @param cl the parsed command line object including options
   * @throws IOException if the command fails
   */
  abstract void runCommand(AlluxioURI path, CommandLine cl) throws IOException;

  @Override
  protected int getNumOfArgs() {
    return 1;
  }

  @Override
  public void run(CommandLine cl) throws IOException {
    String[] args = cl.getArgs();
    AlluxioURI inputPath = new AlluxioURI(args[0]);

    List<AlluxioURI> paths = AlluxioShellUtils.getAlluxioURIs(mFileSystem, inputPath);
    if (paths.size() == 0) { // A unified sanity check on the paths
      throw new IOException(inputPath + " does not exist.");
    }
    Collections.sort(paths, createAlluxioURIComparator());

    List<String> errorMessages = new ArrayList<String>();
    for (AlluxioURI path : paths) {
      try {
        runCommand(path, cl);
      } catch (IOException e) {
        errorMessages.add(e.getMessage());
      }
    }

    if (errorMessages.size() != 0) {
      throw new IOException(Joiner.on('\n').join(errorMessages));
    }
  }

  private static Comparator<AlluxioURI> createAlluxioURIComparator() {
    return new Comparator<AlluxioURI>() {
      @Override
      public int compare(AlluxioURI tUri1, AlluxioURI tUri2) {
        // ascending order
        return tUri1.getPath().compareTo(tUri2.getPath());
      }
    };
  }
}
