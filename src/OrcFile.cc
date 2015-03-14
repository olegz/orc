/**
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

#include "../hdfsaccess/hdfs.h"
#include "orc/OrcFile.hh"
#include "Exceptions.hh"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#ifndef _WIN32
// O_BINARY is windows specific, without which reader will not work.
// So fake a flag here for poxis platform.
// TODO: move this adapter somewhere else better?
#define O_BINARY 0
#endif

namespace orc {

  class FileInputStream : public InputStream {
  private:
    std::string filename ;
    int file;
    int64_t totalLength;

  public:
    FileInputStream(std::string _filename) {
      filename = _filename ;
      file = open(filename.c_str(), O_RDONLY | O_BINARY);
      if (file == -1) {
        throw ParseError("Can't open " + filename);
      }
      struct stat fileStat;
      if (fstat(file, &fileStat) == -1) {
        throw ParseError("Can't stat " + filename);
      }
      totalLength = fileStat.st_size;
    }

    ~FileInputStream();

    int64_t getLength() const {
      return totalLength;
    }

    void read(void* buffer, uint64_t offset,
              uint64_t length) override {
      ssize_t bytesRead = pread(file, buffer, length,
                                static_cast<off_t>(offset));
      if (bytesRead == -1) {
        throw ParseError("Bad read of " + filename);
      }
      if (static_cast<unsigned long>(bytesRead) != length) {
        throw ParseError("Short read of " + filename);
      }
    }

    const std::string& getName() const override { 
      return filename;
    }
  };

  FileInputStream::~FileInputStream() { 
    close(file);
  }

  class HdfsFileInputStream : public InputStream {
  private:
    std::string filename;
    int64_t totalLength;
    hdfsFS fs;
    hdfsFileInfo* info;
    hdfsFile file;

  public:
    HdfsFileInputStream(std::string _namenode, std::string _filename) {
      filename = _filename;
      struct hdfsBuilder* bld = hdfsNewBuilder();
      hdfsBuilderSetNameNode(bld, _namenode.c_str());
      hdfsBuilderSetForceNewInstance(bld);
      fs = hdfsBuilderConnect(bld);
      hdfsFreeBuilder(bld);
      if (fs == nullptr) {
        throw ParseError("Cannot connect to " + _namenode);
      }
      info = hdfsGetPathInfo(fs, _filename.c_str());
      if (info == nullptr) {
        throw ParseError("Cannot stats HDFS file " + _filename);
      }
      if (info->mKind == kObjectKindDirectory) {
        throw ParseError("Cannot parse a directory " + _filename);
      }
      totalLength = info->mSize;
      file = hdfsOpenFile(fs, _filename.c_str(), O_RDONLY, 0, 0, 0);
      if (file == nullptr) {
        throw ParseError("Cannot open file " + _filename);
      }
    }

    ~HdfsFileInputStream() {
      hdfsFreeFileInfo(info, 1);
      hdfsCloseFile(fs, file);
      hdfsDisconnect(fs);
    }

    int64_t getLength() const {
      return totalLength;
    }

    const std::string& getName() const override {
      return filename;
    }

    void read(void* buffer, uint64_t offset,
              uint64_t length) override {
      if (hdfsSeek(fs, file, offset) < 0) {
        throw ParseError("Seek error on file " + filename);
      }
      tSize bytesRead = 0;
      uint64_t total = 0;
      while (total < length) {
        bytesRead = hdfsRead(fs, file, static_cast<char*>(buffer) + total,
                             length - total);
        if (bytesRead == -1) {
          throw ParseError("Bad read of " + filename);
        }
        total += bytesRead;
      }
      if (hdfsSeek(fs, file, offset) < 0) {
        throw ParseError("Seek error on file " + filename);
      }
    }
  };

  std::unique_ptr<InputStream> readLocalFile(const std::string& path) {
    return std::unique_ptr<InputStream>(new FileInputStream(path));
  }

  std::unique_ptr<InputStream> readHdfsFile(
      const std::string& namenode,
      const std::string& path) {
    return std::unique_ptr<InputStream>(
        new HdfsFileInputStream(namenode, path));
  }

}
