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
#include "Exceptions.hh"

#include <string>
#include <memory>
#include <iostream>
#include <string>

int main(int argc, char* argv[]) {
  if (argc < 2) {
    std::cout << "Usage: file-scan <filename>\n";
  }

  orc::ReaderOptions opts;
  std::list<int> cols;
  cols.push_back(0);
  opts.include(cols);

  std::unique_ptr<orc::Reader> reader;
  try{
    reader = orc::createReader(orc::readLocalFile(std::string(argv[1])), opts);
  } catch (orc::ParseError e) {
    std::cout << "Error reading file " << argv[1] << "! "
              << e.what() << std::endl;
    return -1;
  }

  const int BATCH_SIZE = 1000;
  std::unique_ptr<orc::ColumnVectorBatch> batch = reader->createRowBatch(BATCH_SIZE);

  // Calculate batch memory usage
  uint64_t memory = BATCH_SIZE * sizeof(char);  // batch->notNull
  const orc::Type& schema = reader->getType();
  const std::vector<bool> selectedColumns = reader->getSelectedColumns();
  for (unsigned int i=0; i < schema.getSubtypeCount(); i++) {
    if (selectedColumns[i+1]) {
      switch (static_cast<unsigned int>(schema.getSubtype(i).getKind())) {
      case orc::BOOLEAN:
      case orc::BYTE:
      case orc::SHORT:
      case orc::INT:
      case orc::LONG:
      case orc::DATE: {
        memory += BATCH_SIZE*(sizeof(int64_t) + sizeof(char)) ;
        break;
      }
      case orc::TIMESTAMP: {
        memory += BATCH_SIZE*(sizeof(int64_t) + sizeof(char)
            + 2*sizeof(int64_t)); // TimestampColumnReader uses temp buffers
        break;
      }

      case orc::FLOAT:
      case orc::DOUBLE: {
        memory += BATCH_SIZE*(sizeof(double) + sizeof(char)) ;
        break;
      }
      case orc::STRING:
      case orc::BINARY:
      case orc::CHAR:
      case orc::VARCHAR: {
        memory += BATCH_SIZE*(sizeof(char*) + sizeof(int64_t) + sizeof(char)) ;
        break;
      }
      case orc::DECIMAL: {
        if (schema.getSubtype(i).getPrecision() == 0 ||
            schema.getSubtype(i).getPrecision() > 18) {
          memory += BATCH_SIZE * (sizeof(orc::Int128) +
              sizeof(int64_t) + sizeof(char));
        } else {
          memory += BATCH_SIZE * (2*sizeof(int64_t) + sizeof(char));
        }
        break;
      }
      case orc::STRUCT:
      case orc::LIST:
      case orc::MAP:
      case orc::UNION: {
        throw orc::NotImplementedYet("Complex datatypes are not supported yet");
      }
      default:
        break;
      }
    }
  };

  unsigned long rows = 0;
  unsigned long batches = 0;
  while (reader->next(*batch)) {
    batches += 1;
    rows += batch->numElements;
  }
  std::cout << "Rows: " << rows << std::endl;
  std::cout << "Batches: " << batches << std::endl;

  std::cout << "Total memory estimate: " << memory+reader->memoryEstimate() << std::endl;

  return 0;
}
