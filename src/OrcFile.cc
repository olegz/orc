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

#include "orc/OrcFile.hh"
#include "Exceptions.hh"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

namespace orc {

  class FileInputStream : public InputStream {
  private:
    std::string filename ;
    int file;
    off_t totalLength;

  public:
    FileInputStream(std::string _filename) {
      filename = _filename ;
      file = open(filename.c_str(), O_RDONLY);
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

    long getLength() const {
      return totalLength;
    }

    void read(void* buffer, unsigned long offset,
              unsigned long length) override {
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

  std::unique_ptr<InputStream> readLocalFile(const std::string& path) {
    return std::unique_ptr<InputStream>(new FileInputStream(path));
  }
}
