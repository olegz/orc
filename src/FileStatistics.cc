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
    std::cout << "Usage: file-metadata <filename>\n";
  }

  orc::ReaderOptions opts;
  std::list<int> cols;
  cols.push_back(0);
  opts.include(cols);

  std::unique_ptr<orc::MemoryPool> pool(orc::createTestMemoryPool().release());
  std::unique_ptr<orc::Reader> reader;
  try{
    reader = orc::createReader(orc::readLocalFile(std::string(argv[1])), opts, pool.get());
  } catch (orc::ParseError e) {
    std::cout << "Error reading file " << argv[1] << "! "
              << e.what() << std::endl;
    return -1;
  }

 // print out all selected columns statistics.
 std::list<orc::ColumnStatistics*> colStats = reader->getStatistics();
 std::cout << "File " << argv[1] << " has " << colStats.size() << " columns"  << std::endl;
 int i = 0;
 for(std::list<orc::ColumnStatistics*>::const_iterator iter = colStats.begin();
     iter != colStats.end(); iter++,i++) {
   std::cout << "*** Column " << i << " ***" << std::endl;
   std::cout << (*iter)->toString() << std::endl;
 }

 // test stripe statistics
 std::unique_ptr<orc::StripeStatistics> stripeStats;
 std::cout << "File " << argv[1] << " has " << reader->getNumberOfStripes() << " stripes"  << std::endl;
 for (unsigned int j = 0; j < reader->getNumberOfStripes(); j++) {
   stripeStats = reader->getStripeStatistics(j);
   std::cout << "*** Stripe " << j << " ***" << std::endl << std::endl ;

   for(unsigned int k = 0; k < stripeStats->getNumberOfColumnStatistics(); ++k){
     std::cout << "--- Column " << k << " ---" << std::endl;
     std::cout << stripeStats->getColumnStatisticsInStripe(k)->toString() << std::endl;
   }
 }

  return 0;
}
