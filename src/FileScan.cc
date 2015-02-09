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
#include <typeinfo>

int main(int argc, char* argv[]) {
  if (argc < 2) {
    std::cout << "Usage: file-scan <filename>\n";
  }
  orc::ReaderOptions opts;
  std::list<int> cols;
  // TODO: 0 is for first column? or the metadata column?
  cols.push_back(0);
  // got confused by index, start from 0 or 1?
  for(int i = 1; i <= 8; i++){
      cols.push_back(i);
  }
  opts.include(cols);

  std::unique_ptr<orc::Reader> reader;
  try{
    reader = orc::createReader(orc::readLocalFile(std::string(argv[1])), opts);
  } catch (orc::ParseError e) {
    std::cout << "Error reading file " << argv[1] << "! "
              << e.what() << std::endl;
    return -1;
  }

  const orc::Type& schema = reader->getType();
  std::list<orc::ColumnStatistics*> colStats = reader->getStatistics();
  std::cout << "file has " << colStats.size() << "col statistics\n";

  int columnIdx = 0;
  for(std::list<orc::ColumnStatistics*>::const_iterator iter = colStats.begin(); iter != colStats.end(); iter++){
      std::cout << "Column " << columnIdx << " has " << (*iter)->getNumberOfValues() << " values\n";
      if(typeid(**iter) == typeid(orc::DateColumnStatistics)){
          std::cout << "col data type is date\n";
          const orc::DateColumnStatistics &dateCol = dynamic_cast<const orc::DateColumnStatistics&> (**iter);
          std::cout << "Minimum is " << dateCol.getMinimum() << std::endl
                    << "Maximum is " << dateCol.getMaximum() << std::endl;
      }else if(typeid(**iter) == typeid(orc::IntegerColumnStatistics)){
          std::cout << "col data type is Integer\n";
          const orc::IntegerColumnStatistics &intCol = dynamic_cast<const orc::IntegerColumnStatistics&> (**iter);
          std::cout << "Minimum is " << intCol.getMinimum() << std::endl
                    << "Maximum is " << intCol.getMaximum() << std::endl;
      }else if(typeid(**iter) == typeid(orc::StringColumnStatistics)){
          std::cout << "col data type is string\n";
          const orc::StringColumnStatistics &stringCol = dynamic_cast<const orc::StringColumnStatistics&> (**iter);
          std::cout << "Minimum is " << stringCol.getMinimum() << std::endl
                    << "Maximum is " << stringCol.getMaximum() << std::endl;
      }
      //TODO: add more
      columnIdx++;
  }


  std::unique_ptr<orc::ColumnVectorBatch> batch = reader->createRowBatch(1000);
  unsigned long rows = 0;
  unsigned long batches = 0;
  while (reader->next(*batch)) {
    batches += 1;
    rows += batch->numElements;
  }
  
  std::cout << "Rows: " << rows << "\n";
  std::cout << "Batches: " << batches << "\n";
  return 0;
}
