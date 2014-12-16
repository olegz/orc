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

#include "Exceptions.hh"
#include "orc/Vector.hh"

#include <iostream>
#include <sstream>

namespace orc {

  ColumnVectorBatch::ColumnVectorBatch(unsigned long cap
                                       ): notNull(new char[cap]) {
    capacity = cap;
    numElements = 0;
    hasNulls = false;
  }

  ColumnVectorBatch::~ColumnVectorBatch() {
    // PASS
  }

  LongVectorBatch::LongVectorBatch(unsigned long capacity
                                   ): ColumnVectorBatch(capacity),
                                      data(std::unique_ptr<long[]>
                                           (new long[capacity])){
    // PASS
  }

  LongVectorBatch::~LongVectorBatch() {
    // PASS
  }
  
  std::string LongVectorBatch::toString() const {
    std::ostringstream buffer;
    buffer << "Long vector <" << numElements << " of " << capacity << ">";
    return buffer.str();
  }

  DoubleVectorBatch::DoubleVectorBatch(unsigned long capacity
                                       ): ColumnVectorBatch(capacity),
                                          data(std::unique_ptr<double[]>
                                           (new double[capacity])){
    // PASS
  }

  DoubleVectorBatch::~DoubleVectorBatch() {
    // PASS
  }

  std::string DoubleVectorBatch::toString() const {
    std::ostringstream buffer;
    buffer << "Double vector <" << numElements << " of " << capacity << ">";
    return buffer.str();
  }

  StringVectorBatch::StringVectorBatch(unsigned long capacity
                                       ): ColumnVectorBatch(capacity),
                                          data(std::unique_ptr<char*[]>
                                               (new char *[capacity])),
                                          length(std::unique_ptr<long[]>
                                                 (new long[capacity])) {
    // PASS
  }

  StringVectorBatch::~StringVectorBatch() {
    // PASS
  }

  std::string StringVectorBatch::toString() const {
    std::ostringstream buffer;
    buffer << "Byte vector <" << numElements << " of " << capacity << ">";
    return buffer.str();
  }

  StructVectorBatch::StructVectorBatch(unsigned long capacity
                                       ): ColumnVectorBatch(capacity) {
    // PASS
  }

  StructVectorBatch::~StructVectorBatch() {
    // PASS
  }

  std::string StructVectorBatch::toString() const {
    std::ostringstream buffer;
    buffer << "Struct vector <" << numElements << " of " << capacity 
           << "; ";
    for(unsigned int i=0; i < numFields; ++i) {
      buffer << fields[i]->toString() << "; ";
    }
    buffer << ">";
    return buffer.str();
  }
}
