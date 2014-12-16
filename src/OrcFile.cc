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

#include <fstream>

namespace orc {

  class FileInputStream : public InputStream {
  private:
    std::string filename ;
    std::ifstream file;
    long totalLength;

  public:
    FileInputStream(std::string _filename) {
      filename = _filename ;
      file.open(filename.c_str(), std::ios::in | std::ios::binary);
      file.seekg(0,file.end);
      totalLength = file.tellg();
    }

    ~FileInputStream();

    long getLength() const {
      return totalLength;
    }

    void read(void* buffer, unsigned long offset,
              unsigned long length) override {
      file.seekg(static_cast<std::streamoff>(offset));
      file.read(static_cast<char*>(buffer), 
                static_cast<std::streamsize>(length));
    }

    const std::string& getName() const override { 
      return filename;
    }
  };

  FileInputStream::~FileInputStream() { 
    file.close();
  }

  std::unique_ptr<InputStream> readLocalFile(const std::string& path) {
    return std::unique_ptr<InputStream>(new FileInputStream(path));
  }
}
