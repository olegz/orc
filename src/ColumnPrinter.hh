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

#ifndef ORC_COLUMN_PRINTER_HH
#define ORC_COLUMN_PRINTER_HH

#include "orc/OrcFile.hh"

#include <string>
#include <memory>
#include <iostream>
#include <string>

namespace orc {

    class ColumnPrinter {
    public:
      virtual void printRow(unsigned long rowId) = 0;
      virtual ~ColumnPrinter();
    };


    class StructColumnPrinter: public ColumnPrinter {
    private:
      ColumnPrinter** fields;
      unsigned long numFields;
    public:
      StructColumnPrinter(const ColumnVectorBatch& batch);
      virtual ~StructColumnPrinter() = default;
      void printRow(unsigned long rowId) override;
    };
}
#endif

