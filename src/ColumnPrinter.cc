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

#include "ColumnPrinter.hh"
#include <typeinfo>
#include <stdexcept>

namespace orc {

  class LongColumnPrinter: public ColumnPrinter {
  private:
    const long* data;
  public:
    LongColumnPrinter(const ColumnVectorBatch& batch);
    ~LongColumnPrinter() {}
    void printRow(unsigned long rowId)  ;
    void reset(const ColumnVectorBatch& batch)  ;
  };

  class DoubleColumnPrinter: public ColumnPrinter {
  private:
    const double* data;
  public:
    DoubleColumnPrinter(const ColumnVectorBatch& batch);
    virtual ~DoubleColumnPrinter() {}
    void printRow(unsigned long rowId)  ;
    void reset(const ColumnVectorBatch& batch)  ;
  };

  class StringColumnPrinter: public ColumnPrinter {
  private:
    const char* const * start;
    const long* length;
  public:
    StringColumnPrinter(const ColumnVectorBatch& batch);
    virtual ~StringColumnPrinter() {}
    void printRow(unsigned long rowId)  ;
    void reset(const ColumnVectorBatch& batch)  ;
  };

  ColumnPrinter::~ColumnPrinter() {
    // PASS
  }

  LongColumnPrinter::LongColumnPrinter(const  ColumnVectorBatch& batch) {
    reset(batch);
  }

  void LongColumnPrinter::reset(const  ColumnVectorBatch& batch) {
    data = dynamic_cast<const LongVectorBatch&>(batch).data.data();
  }

  void LongColumnPrinter::printRow(unsigned long rowId) {
    std::cout << data[rowId];
  }

  DoubleColumnPrinter::DoubleColumnPrinter(const  ColumnVectorBatch& batch) {
    reset(batch);
  }

  void DoubleColumnPrinter::reset(const  ColumnVectorBatch& batch) {
    data = dynamic_cast<const DoubleVectorBatch&>(batch).data.data();
  }

  void DoubleColumnPrinter::printRow(unsigned long rowId) {
    std::cout << data[rowId];
  }

  StringColumnPrinter::StringColumnPrinter(const ColumnVectorBatch& batch) {
    reset(batch);
  }

  void StringColumnPrinter::reset(const ColumnVectorBatch& batch) {
    start = dynamic_cast<const StringVectorBatch&>(batch).data.data();
    length = dynamic_cast<const StringVectorBatch&>(batch).length.data();
  }

  void StringColumnPrinter::printRow(unsigned long rowId) {
    std::cout.write(start[rowId], length[rowId]);
  }

  StructColumnPrinter::StructColumnPrinter(const ColumnVectorBatch& batch) {
    const StructVectorBatch& structBatch =
      dynamic_cast<const StructVectorBatch&>(batch);
    for(std::vector<ColumnVectorBatch*>::const_iterator ptr=structBatch.fields.begin();
        ptr != structBatch.fields.end(); ++ptr) {
      if (typeid(**ptr) == typeid(LongVectorBatch)) {
        fields.push_back(new LongColumnPrinter(**ptr));
      } else if (typeid(**ptr) == typeid(DoubleVectorBatch)) {
        fields.push_back(new DoubleColumnPrinter(**ptr));
      } else if (typeid(**ptr) == typeid(StringVectorBatch)) {
        fields.push_back(new StringColumnPrinter(**ptr));
      } else if (typeid(**ptr) == typeid(StructVectorBatch)) {
        fields.push_back(new StructColumnPrinter(**ptr));
      } else {
        throw std::logic_error("unknown batch type");
      }
    }
  }

  StructColumnPrinter::~StructColumnPrinter() {
    for (size_t i = 0; i < fields.size(); i++) {
      delete fields[i];
    }
  }

  void StructColumnPrinter::reset(const ColumnVectorBatch& batch) {
    const StructVectorBatch& structBatch =
      dynamic_cast<const StructVectorBatch&>(batch);
    for(size_t i=0; i < fields.size(); ++i) {
      fields[i]->reset(*(structBatch.fields[i]));
    }
  }

  void StructColumnPrinter::printRow(unsigned long rowId) {
    if (fields.size() > 0) {
      for (std::vector<ColumnPrinter*>::iterator ptr = fields.begin(); ptr != fields.end(); ++ptr) {
        (*ptr)->printRow(rowId);
        std::cout << "\t";
      }
      std::cout << "\n";
    }
  }
}
