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

package alluxio.web;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.Constants;
import alluxio.PropertyKey;
import alluxio.client.ReadType;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.exception.AccessControlException;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.master.MasterProcess;
import alluxio.master.block.BlockMaster;
import alluxio.master.file.FileSystemMaster;
import alluxio.security.LoginUser;
import alluxio.security.authentication.AuthenticatedClientUser;
import alluxio.util.SecurityUtils;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.FileInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Servlet that provides data for browsing the file system.
 */
@ThreadSafe
public final class WebInterfaceBrowseServlet extends HttpServlet {

  private static final long serialVersionUID = 6121623049981468871L;

  private final transient MasterProcess mMasterProcess;

  /**
   * Creates a new instance of {@link WebInterfaceBrowseServlet}.
   *
   * @param masterProcess the Alluxio master process
   */
  public WebInterfaceBrowseServlet(MasterProcess masterProcess) {
    mMasterProcess = masterProcess;
  }

  /**
   * This function displays 5KB of a file from a specific offset if it is in ASCII format.
   *
   * @param path the path of the file to display
   * @param request the {@link HttpServletRequest} object
   * @param offset where the file starts to display
   * @throws FileDoesNotExistException if the file does not exist
   * @throws InvalidPathException if an invalid path is encountered
   */
  private void displayFile(AlluxioURI path, HttpServletRequest request, long offset)
      throws FileDoesNotExistException, InvalidPathException, IOException, AlluxioException {
    FileSystem fs = FileSystem.Factory.get();
    String fileData;
    URIStatus status = fs.getStatus(path);
    if (status.isCompleted()) {
      OpenFileOptions options = OpenFileOptions.defaults().setReadType(ReadType.NO_CACHE);
      try (FileInStream is = fs.openFile(path, options)) {
        int len = (int) Math.min(5 * Constants.KB, status.getLength() - offset);
        byte[] data = new byte[len];
        long skipped = is.skip(offset);
        if (skipped < 0) {
          // nothing was skipped
          fileData = "Unable to traverse to offset; is file empty?";
        } else if (skipped < offset) {
          // couldn't skip all the way to offset
          fileData = "Unable to traverse to offset; is offset larger than the file?";
        } else {
          // read may not read up to len, so only convert what was read
          int read = is.read(data, 0, len);
          if (read < 0) {
            // stream couldn't read anything, skip went to EOF?
            fileData = "Unable to read file";
          } else {
            fileData = WebUtils.convertByteArrayToStringWithoutEscape(data, 0, read);
          }
        }
      }
    } else {
      fileData = "The requested file is not complete yet.";
    }
    List<UIFileBlockInfo> uiBlockInfo = new ArrayList<>();
    for (FileBlockInfo fileBlockInfo : mMasterProcess.getMaster(FileSystemMaster.class)
        .getFileBlockInfoList(path)) {
      uiBlockInfo.add(new UIFileBlockInfo(fileBlockInfo));
    }
    request.setAttribute("fileBlocks", uiBlockInfo);
    request.setAttribute("fileData", fileData);
    request.setAttribute("highestTierAlias",
        mMasterProcess.getMaster(BlockMaster.class).getGlobalStorageTierAssoc().getAlias(0));
  }

  /**
   * Populates attribute fields with data from the MasterInfo associated with this servlet. Errors
   * will be displayed in an error field. Debugging can be enabled to display additional data. Will
   * eventually redirect the request to a jsp.
   *
   * @param request the {@link HttpServletRequest} object
   * @param response the {@link HttpServletResponse} object
   * @throws ServletException if the target resource throws this exception
   * @throws IOException if the target resource throws this exception
   */
  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    if (SecurityUtils.isSecurityEnabled() && AuthenticatedClientUser.get() == null) {
      AuthenticatedClientUser.set(LoginUser.get().getName());
    }
    request.setAttribute("debug", Configuration.getBoolean(PropertyKey.DEBUG));
    request.setAttribute("showPermissions",
        Configuration.getBoolean(PropertyKey.SECURITY_AUTHORIZATION_PERMISSION_ENABLED));

    request.setAttribute("masterNodeAddress", mMasterProcess.getRpcAddress().toString());
    request.setAttribute("invalidPathError", "");
    List<FileInfo> filesInfo;
    String requestPath = request.getParameter("path");
    if (requestPath == null || requestPath.isEmpty()) {
      requestPath = AlluxioURI.SEPARATOR;
    }
    AlluxioURI currentPath = new AlluxioURI(requestPath);
    request.setAttribute("currentPath", currentPath.toString());
    request.setAttribute("viewingOffset", 0);
    FileSystemMaster fileSystemMaster = mMasterProcess.getMaster(FileSystemMaster.class);

    try {
      long fileId = fileSystemMaster.getFileId(currentPath);
      FileInfo fileInfo = fileSystemMaster.getFileInfo(fileId);
      UIFileInfo currentFileInfo = new UIFileInfo(fileInfo);
      if (currentFileInfo.getAbsolutePath() == null) {
        throw new FileDoesNotExistException(currentPath.toString());
      }
      request.setAttribute("currentDirectory", currentFileInfo);
      request.setAttribute("blockSizeBytes", currentFileInfo.getBlockSizeBytes());
      if (currentFileInfo.getIsDirectory()) {
        getServletContext().getRequestDispatcher("/browse.jsp").forward(request, response);
        return;
      } else {
        String offsetParam = request.getParameter("offset");
        long relativeOffset = 0;
        long offset;
        try {
          if (offsetParam != null) {
            relativeOffset = Long.parseLong(offsetParam);
          }
        } catch (NumberFormatException e) {
          relativeOffset = 0;
        }
        String endParam = request.getParameter("end");
        // If no param "end" presents, the offset is relative to the beginning; otherwise, it is
        // relative to the end of the file.
        if (endParam == null) {
          offset = relativeOffset;
        } else {
          offset = fileInfo.getLength() - relativeOffset;
        }
        if (offset < 0) {
          offset = 0;
        } else if (offset > fileInfo.getLength()) {
          offset = fileInfo.getLength();
        }
        try {
          displayFile(new AlluxioURI(currentFileInfo.getAbsolutePath()), request, offset);
        } catch (AlluxioException e) {
          throw new IOException(e);
        }
        request.setAttribute("viewingOffset", offset);
        getServletContext().getRequestDispatcher("/viewFile.jsp").forward(request, response);
        return;
      }

    } catch (FileDoesNotExistException e) {
      request.setAttribute("invalidPathError", "Error: Invalid Path " + e.getMessage());
      getServletContext().getRequestDispatcher("/browse.jsp").forward(request, response);
      return;
    } catch (IOException e) {
      request.setAttribute("invalidPathError",
          "Error: File " + currentPath + " is not available " + e.getMessage());
      getServletContext().getRequestDispatcher("/browse.jsp").forward(request, response);
      return;
    } catch (AccessControlException e) {
      request.setAttribute("invalidPathError",
          "Error: File " + currentPath + " cannot be accessed " + e.getMessage());
      getServletContext().getRequestDispatcher("/browse.jsp").forward(request, response);
      return;
    }
  }
}
