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

namespace orc {

  class LongColumnPrinter: public ColumnPrinter {
  private:
    const long* data;
  public:
    LongColumnPrinter(const ColumnVectorBatch& batch);
    ~LongColumnPrinter() = default;
    void printRow(unsigned long rowId) override;
    void reset(const ColumnVectorBatch& batch) override;
  };

  class DoubleColumnPrinter: public ColumnPrinter {
  private:
    const double* data;
  public:
    DoubleColumnPrinter(const ColumnVectorBatch& batch);
    virtual ~DoubleColumnPrinter() = default;
    void printRow(unsigned long rowId) override;
    void reset(const ColumnVectorBatch& batch) override;
  };

  class StringColumnPrinter: public ColumnPrinter {
  private:
    const char* const * start;
    const long* length;
  public:
    StringColumnPrinter(const ColumnVectorBatch& batch);
    virtual ~StringColumnPrinter() = default;
    void printRow(unsigned long rowId) override;
    void reset(const ColumnVectorBatch& batch) override;
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
    for(auto ptr=structBatch.fields.cbegin();
        ptr != structBatch.fields.cend(); ++ptr) {
      if (typeid(*(ptr->get())) == typeid(LongVectorBatch)) {
        fields.push_back(std::unique_ptr<ColumnPrinter>
                         (new LongColumnPrinter(*(ptr->get()))));
      } else if (typeid(*(ptr->get())) == typeid(DoubleVectorBatch)) {
        fields.push_back(std::unique_ptr<ColumnPrinter>
                         (new DoubleColumnPrinter(*(ptr->get()))));
      } else if (typeid(*(ptr->get())) == typeid(StringVectorBatch)) {
        fields.push_back(std::unique_ptr<ColumnPrinter>
                         (new StringColumnPrinter(*(ptr->get()))));
      } else if (typeid(*(ptr->get())) == typeid(StructVectorBatch)) {
        fields.push_back(std::unique_ptr<ColumnPrinter>
                         (new StructColumnPrinter(*(ptr->get()))));
      } else {
        throw std::logic_error("unknown batch type");
      }
    }
  }

  void StructColumnPrinter::reset(const ColumnVectorBatch& batch) {
    const StructVectorBatch& structBatch =
      dynamic_cast<const StructVectorBatch&>(batch);
    for(size_t i=0; i < fields.size(); ++i) {
      fields[i].get()->reset(*(structBatch.fields[i].get()));
    }
  }

  void StructColumnPrinter::printRow(unsigned long rowId) {
    if (fields.size() > 0) {
      fields[0]->printRow(rowId);
      for (auto ptr = fields.cbegin(); ptr != fields.cend(); ++ptr) {
        std::cout << "\t";
        ptr->get()->printRow(rowId);
      }
      std::cout << "\n";
    }
  }
}
