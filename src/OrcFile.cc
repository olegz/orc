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
#include <algorithm>

namespace orc {

  class FileInputStream : public InputStream {
  private:
    std::string filename ;
    int file;
    off_t totalLength;

    char* page;
    unsigned long pageStart ;
    unsigned long pageLength ;

  public:
    static const unsigned long PAGE_SIZE = 4096 ;

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

      page = new char[PAGE_SIZE] ;
      pageStart = pageLength = 0;
    }

    ~FileInputStream() {
      close(file);
      delete[] page ;
    }

    long getLength() const {
      return totalLength;
    }

    void read(void* buffer, unsigned long offset,
              unsigned long length) override {

      // Check if the data is available
      if (offset >= pageStart && offset+length <= pageStart+pageLength) {
        std::memcpy(buffer, page+(offset-pageStart), length);
        return ;
      }

      ssize_t bytesRead ;
      if (length < PAGE_SIZE) {
        unsigned long availableBytes = static_cast<unsigned long>(totalLength)-offset;
        if (availableBytes > PAGE_SIZE) {
          availableBytes = PAGE_SIZE;
        }
        bytesRead = pread(file, page, availableBytes, static_cast<off_t>(offset));
        if (bytesRead == -1) {
          throw ParseError("Bad read of " + filename);
        }
        if (static_cast<unsigned long>(bytesRead) != availableBytes) {
          throw ParseError("Short read of " + filename);
        }
        pageStart = offset ;
        pageLength = availableBytes;
        std::memcpy(buffer, page, length);
      } else {
        bytesRead = pread(file, buffer, length, static_cast<off_t>(offset));
        if (bytesRead == -1) {
          throw ParseError("Bad read of " + filename);
        }
        if (static_cast<unsigned long>(bytesRead) != length) {
          throw ParseError("Short read of " + filename);
        }
        std::memcpy(page, static_cast<char*>(buffer)+(length-PAGE_SIZE), PAGE_SIZE);
        pageStart = offset+length-PAGE_SIZE;
        pageLength = PAGE_SIZE;
      }
    }

    const std::string& getName() const override { 
      return filename;
    }
  };

  std::unique_ptr<InputStream> readLocalFile(const std::string& path) {
    return std::unique_ptr<InputStream>(new FileInputStream(path));
  }
}
