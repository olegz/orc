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
    };

    class DoubleColumnPrinter: public ColumnPrinter {
    private:
      const double* data;
    public:
      DoubleColumnPrinter(const ColumnVectorBatch& batch);
      virtual ~DoubleColumnPrinter() = default;
      void printRow(unsigned long rowId) override;
    };

    class StringColumnPrinter: public ColumnPrinter {
    private:
      char** start;
      long* length;
    public:
      StringColumnPrinter(const ColumnVectorBatch& batch);
      virtual ~StringColumnPrinter() = default;
      void printRow(unsigned long rowId) override;
    };

    ColumnPrinter::~ColumnPrinter() {
    }

    LongColumnPrinter::LongColumnPrinter(const  ColumnVectorBatch& batch) {
      data = dynamic_cast<const  LongVectorBatch&>(batch).data.get();
    }

    void LongColumnPrinter::printRow(unsigned long rowId) {
      std::cout << data[rowId];
    }

    DoubleColumnPrinter::DoubleColumnPrinter(const  ColumnVectorBatch& batch) {
      data = dynamic_cast<const  DoubleVectorBatch&>(batch).data.get();
    }

    void DoubleColumnPrinter::printRow(unsigned long rowId) {
      std::cout << data[rowId];
    }

    StringColumnPrinter::StringColumnPrinter(const  ColumnVectorBatch& batch) {
      start = dynamic_cast<const  StringVectorBatch&>(batch).data.get();
      length = dynamic_cast<const  StringVectorBatch&>(batch).length.get();
    }

    void StringColumnPrinter::printRow(unsigned long rowId) {
      std::cout.write(start[rowId], length[rowId]);
    }

    StructColumnPrinter::StructColumnPrinter(const ColumnVectorBatch& batch) {
      const StructVectorBatch& structBatch =
          dynamic_cast<const StructVectorBatch&>(batch);
      numFields = structBatch.numFields;
      fields = new ColumnPrinter*[numFields];
      for(unsigned int i=0; i < numFields; ++i) {
        const  ColumnVectorBatch& subBatch = *(structBatch.fields.get()[i]);
        if (typeid(subBatch) == typeid(LongVectorBatch)) {
          fields[i] = new LongColumnPrinter(subBatch);
        } else if (typeid(subBatch) == typeid(DoubleVectorBatch)) {
          fields[i] = new DoubleColumnPrinter(subBatch);
        } else if (typeid(subBatch) == typeid(StringVectorBatch)) {
          fields[i] = new StringColumnPrinter(subBatch);
        } else if (typeid(subBatch) == typeid(StructVectorBatch)) {
          fields[i] = new StructColumnPrinter(subBatch);
        } else {
          throw std::logic_error("unknown batch type");
        }
      }
    }

    void StructColumnPrinter::printRow(unsigned long rowId) {
      if (numFields > 0) {
        fields[0]->printRow(rowId);
        for (unsigned long i=1; i < numFields; ++i) {
          std::cout << "\t";
          fields[i]->printRow(rowId);
        }
        std::cout << "\n";
      }
    }
}

