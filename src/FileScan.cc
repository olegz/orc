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

 // print out all selected columns statistics.
 std::list<orc::ColumnStatistics*> colStats = reader->getStatistics();
 std::cout << "File has " << colStats.size() << " col statistics\n" << "{" << std::endl;
 int i = 0;
 for(std::list<orc::ColumnStatistics*>::const_iterator iter = colStats.begin();
     iter != colStats.end(); iter++, i++) {
     std::cout << (*iter)->toString() << std::endl;
     std::cout << std::endl;
 }
 std::cout << "}" << std::endl;

 // test print out one column statistics.
 // e.g. print the forth column(col = 3)
 int col = 3;
 std::cout << "Get statistics of column " << col+1 << std::endl;
 std::cout << reader->getColumnStatistics(col)->toString() << std::endl;

 // test stripe statistics
 std::cout << std::endl << "File has "
     << reader->getNumberOfStripes() << "stripes \n";
 unsigned long stripe = 60;
 if(stripe < reader->getNumberOfStripes()) {
   std::unique_ptr<orc::StripeStatistics> stripeStats =
       reader->getStripeStatistics(stripe);

   std::cout << "Stripe " << stripe << " has "
             << stripeStats->getNumberOfColumnStatistics() << "columns \n" << "{" << std::endl;

   for(uint i = 0; i < stripeStats->getNumberOfColumnStatistics(); ++i){
     std::unique_ptr<orc::ColumnStatistics> colStats =
         stripeStats->getColumnStatisticsInStripe(i);
     std::cout << colStats->toString() << std::endl;
     std::cout << std::endl;
   }
   std::cout << "}" << std::endl;
 }
  
  std::unique_ptr<orc::ColumnVectorBatch> batch = reader->createRowBatch(1000);
  unsigned long rows = 0;
  unsigned long batches = 0;
  while (reader->next(*batch)) {
    batches += 1;
    rows += batch->numElements;
  }
  std::cout << "Rows: " << rows << std::endl;
  std::cout << "Batches: " << batches << std::endl;
  return 0;
}
