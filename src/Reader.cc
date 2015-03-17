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

#include "orc/Reader.hh"
#include "orc/OrcFile.hh"
#include "ColumnReader.hh"
#include "Exceptions.hh"
#include "RLE.hh"
#include "TypeImpl.hh"
#include "orc/Int128.hh"
#include <google/protobuf/text_format.h>

#include <algorithm>
#include <iostream>
#include <limits>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

namespace orc {

  std::string printProtobufMessage(const google::protobuf::Message& message) {
    std::string result;
    google::protobuf::TextFormat::PrintToString(message, &result);
    return result;
  }

  struct ReaderOptionsPrivate {
    std::list<int> includedColumns;
    unsigned long dataStart;
    unsigned long dataLength;
    unsigned long tailLocation;
    bool throwOnHive11DecimalOverflow;
    int32_t forcedScaleOnHive11Decimal;
    std::ostream* errorStream;
    uint64_t fileBlockSize;

    ReaderOptionsPrivate() {
      includedColumns.assign(1,0);
      dataStart = 0;
      dataLength = std::numeric_limits<unsigned long>::max();
      tailLocation = std::numeric_limits<unsigned long>::max();
      throwOnHive11DecimalOverflow = true;
      forcedScaleOnHive11Decimal = 6;
      errorStream = &std::cerr;
      fileBlockSize = 256*1024 ;
    }
  };

  ReaderOptions::ReaderOptions():
    privateBits(std::unique_ptr<ReaderOptionsPrivate>
                (new ReaderOptionsPrivate())) {
    // PASS
  }

  ReaderOptions::ReaderOptions(const ReaderOptions& rhs):
    privateBits(std::unique_ptr<ReaderOptionsPrivate>
                (new ReaderOptionsPrivate(*(rhs.privateBits.get())))) {
    // PASS
  }

  ReaderOptions::ReaderOptions(ReaderOptions& rhs) {
    // swap privateBits with rhs
    ReaderOptionsPrivate* l = privateBits.release();
    privateBits.reset(rhs.privateBits.release());
    rhs.privateBits.reset(l);
  }

  ReaderOptions& ReaderOptions::operator=(const ReaderOptions& rhs) {
    if (this != &rhs) {
      privateBits.reset(new ReaderOptionsPrivate(*(rhs.privateBits.get())));
    }
    return *this;
  }

  ReaderOptions::~ReaderOptions() {
    // PASS
  }

  ReaderOptions& ReaderOptions::include(const std::list<int>& include) {
    privateBits->includedColumns.assign(include.begin(), include.end());
    return *this;
  }

  ReaderOptions& ReaderOptions::include(std::vector<int> include) {
    privateBits->includedColumns.assign(include.begin(), include.end());
    return *this;
  }

  ReaderOptions& ReaderOptions::range(unsigned long offset,
                                      unsigned long length) {
    privateBits->dataStart = offset;
    privateBits->dataLength = length;
    return *this;
  }

  ReaderOptions& ReaderOptions::setTailLocation(unsigned long offset) {
    privateBits->tailLocation = offset;
    return *this;
  }

  const std::list<int>& ReaderOptions::getInclude() const {
    return privateBits->includedColumns;
  }

  unsigned long ReaderOptions::getOffset() const {
    return privateBits->dataStart;
  }

  unsigned long ReaderOptions::getLength() const {
    return privateBits->dataLength;
  }

  unsigned long ReaderOptions::getTailLocation() const {
    return privateBits->tailLocation;
  }

  ReaderOptions& ReaderOptions::throwOnHive11DecimalOverflow(bool shouldThrow){
    privateBits->throwOnHive11DecimalOverflow = shouldThrow;
    return *this;
  }

  bool ReaderOptions::getThrowOnHive11DecimalOverflow() const {
    return privateBits->throwOnHive11DecimalOverflow;
  }

  ReaderOptions& ReaderOptions::forcedScaleOnHive11Decimal(int32_t forcedScale
                                                           ) {
    privateBits->forcedScaleOnHive11Decimal = forcedScale;
    return *this;
  }

  int32_t ReaderOptions::getForcedScaleOnHive11Decimal() const {
    return privateBits->forcedScaleOnHive11Decimal;
  }

  ReaderOptions& ReaderOptions::setErrorStream(std::ostream& stream) {
    privateBits->errorStream = &stream;
    return *this;
  }

  std::ostream* ReaderOptions::getErrorStream() const {
    return privateBits->errorStream;
  }

  ReaderOptions& ReaderOptions::setFileBlockSize(uint64_t blocksize) {
    privateBits->fileBlockSize = blocksize;
    return *this;
  }

  uint64_t ReaderOptions::getFileBlockSize() {
    return privateBits->fileBlockSize;
  }

  uint64_t ReaderOptions::getFileBlockSize() const {
    return privateBits->fileBlockSize;
  }


  StripeInformation::~StripeInformation() {
    // PASS
  }

  class ColumnStatisticsImpl: public ColumnStatistics {
  private:
    uint64_t valueCount;

  public:
    ColumnStatisticsImpl(const proto::ColumnStatistics& stats);
    virtual ~ColumnStatisticsImpl();

    uint64_t getNumberOfValues() const override {
      return valueCount;
    }

    std::string toString() const override {
      std::ostringstream buffer;
      buffer << "Column has " << valueCount << " values" << std::endl;
      return buffer.str();
    }
  };

  class BinaryColumnStatisticsImpl: public BinaryColumnStatistics {
  private:
    bool _hasTotalLength;
    uint64_t valueCount;
    uint64_t totalLength;

  public:
    BinaryColumnStatisticsImpl(const proto::ColumnStatistics& stats, bool correctStats);
    virtual ~BinaryColumnStatisticsImpl();

    bool hasTotalLength() const override {
      return _hasTotalLength;
    }
    uint64_t getNumberOfValues() const override {
      return valueCount;
    }

    uint64_t getTotalLength() const override {
      if(_hasTotalLength){
        return totalLength;
      }else{
        throw ParseError("Total length is not defined.");
      }
    }

    std::string toString() const override {
      std::ostringstream buffer;
      buffer << "Data type: Binary" << std::endl
             << "Values: " << valueCount << std::endl;
      if(_hasTotalLength){
        buffer << "Total length: " << totalLength << std::endl;
      }else{
        buffer << "Total length: not defined" << std::endl;
      }
      return buffer.str();
    }
  };

  class BooleanColumnStatisticsImpl: public BooleanColumnStatistics {
  private:
    bool _hasCount;
    uint64_t valueCount;
    uint64_t trueCount;

  public:
    BooleanColumnStatisticsImpl(const proto::ColumnStatistics& stats, bool correctStats);
    virtual ~BooleanColumnStatisticsImpl();

    bool hasCount() const override {
      return _hasCount;
    }

    uint64_t getNumberOfValues() const override {
      return valueCount;
    }

    uint64_t getFalseCount() const override {
      if(_hasCount){
        return valueCount - trueCount;
      }else{
        throw ParseError("False count is not defined.");
      }
    }

    uint64_t getTrueCount() const override {
      if(_hasCount){
        return trueCount;
      }else{
        throw ParseError("True count is not defined.");
      }
    }

    std::string toString() const override {
      std::ostringstream buffer;
      buffer << "Data type: Boolean" << std::endl
             << "Values: " << valueCount << std::endl;
      if(_hasCount){
        buffer << "(true: " << trueCount << "; false: "
	       << valueCount - trueCount << ")" << std::endl;
      } else {
        buffer << "(true: not defined; false: not defined)" << std::endl;
        buffer << "True and false count are not defined" << std::endl;
      }
      return buffer.str();
    }
  };

  class DateColumnStatisticsImpl: public DateColumnStatistics {
  private:
    bool _hasMinimum;
    bool _hasMaximum;
    uint64_t valueCount;
    int32_t minimum;
    int32_t maximum;

  public:
    DateColumnStatisticsImpl(const proto::ColumnStatistics& stats, bool correctStats);
    virtual ~DateColumnStatisticsImpl();

    bool hasMinimum() const override {
      return _hasMinimum;
    }

    bool hasMaximum() const override {
      return _hasMaximum;
    }

    uint64_t getNumberOfValues() const override {
      return valueCount;
    }

    int32_t getMinimum() const override {
      if(_hasMinimum){
        return minimum;
      }else{
        throw ParseError("Minimum is not defined.");
      }
    }

    int32_t getMaximum() const override {
      if(_hasMaximum){
        return maximum;
      }else{
        throw ParseError("Maximum is not defined.");
      }
    }

    std::string toString() const override {
      std::ostringstream buffer;
      buffer << "Data type: Date" << std::endl
             << "Values: " << valueCount << std::endl;
      if(_hasMinimum){
        buffer << "Minimum: " << minimum << std::endl;
      }else{
        buffer << "Minimum: not defined" << std::endl;
      }

      if(_hasMaximum){
        buffer << "Maximum: " << maximum << std::endl;
      }else{
        buffer << "Maximum: not defined" << std::endl;
      }
      return buffer.str();
    }
  };

  class DecimalColumnStatisticsImpl: public DecimalColumnStatistics {
  private:
    bool _hasMinimum;
    bool _hasMaximum;
    bool _hasSum;
    uint64_t valueCount;
    std::string minimum;
    std::string maximum;
    std::string sum;

  public:
    DecimalColumnStatisticsImpl(const proto::ColumnStatistics& stats, bool correctStats);
    virtual ~DecimalColumnStatisticsImpl();

    bool hasMinimum() const override {
      return _hasMinimum;
    }

    bool hasMaximum() const override {
      return _hasMaximum;
    }

    bool hasSum() const override {
      return _hasSum;
    }

    uint64_t getNumberOfValues() const override {
      return valueCount;
    }

    Decimal getMinimum() const override {
      if(_hasMinimum){
        return Decimal(minimum);
      }else{
        throw ParseError("Minimum is not defined.");
      }
    }

    Decimal getMaximum() const override {
      if(_hasMaximum){
        return Decimal(maximum);
      }else{
        throw ParseError("Maximum is not defined.");
      }
    }

    Decimal getSum() const override {
      if(_hasSum){
        return Decimal(sum);
      }else{
        throw ParseError("Sum is not defined.");
      }
    }

    std::string toString() const override {
      std::ostringstream buffer;
      buffer << "Data type: Decimal" << std::endl
          << "Values: " << valueCount << std::endl;
      if(_hasMinimum){
        buffer << "Minimum: " << minimum << std::endl;
      }else{
        buffer << "Minimum: not defined" << std::endl;
      }

      if(_hasMaximum){
        buffer << "Maximum: " << maximum << std::endl;
      }else{
        buffer << "Maximum: not defined" << std::endl;
      }

      if(_hasSum){
        buffer << "Sum: " << sum << std::endl;
      }else{
        buffer << "Sum: not defined" << std::endl;
      }

      return buffer.str();
    }
  };

  class DoubleColumnStatisticsImpl: public DoubleColumnStatistics {
  private:
    bool _hasMinimum;
    bool _hasMaximum;
    bool _hasSum;
    uint64_t valueCount;
    double minimum;
    double maximum;
    double sum;

  public:
    DoubleColumnStatisticsImpl(const proto::ColumnStatistics& stats);
    virtual ~DoubleColumnStatisticsImpl();

    bool hasMinimum() const override {
      return _hasMinimum;
    }

    bool hasMaximum() const override {
      return _hasMaximum;
    }

    bool hasSum() const override {
      return _hasSum;
    }

    uint64_t getNumberOfValues() const override {
      return valueCount;
    }

    double getMinimum() const override {
      if(_hasMinimum){
        return minimum;
      }else{
        throw ParseError("Minimum is not defined.");
      }
    }

    double getMaximum() const override {
      if(_hasMaximum){
        return maximum;
      }else{
        throw ParseError("Maximum is not defined.");
      }
    }

    double getSum() const override {
      if(_hasSum){
        return sum;
      }else{
        throw ParseError("Sum is not defined.");
      }
    }

    std::string toString() const override {
      std::ostringstream buffer;
      buffer << "Data type: Double" << std::endl
          << "Values: " << valueCount << std::endl;
      if(_hasMinimum){
        buffer << "Minimum: " << minimum << std::endl;
      }else{
        buffer << "Minimum: not defined" << std::endl;
      }

      if(_hasMaximum){
        buffer << "Maximum: " << maximum << std::endl;
      }else{
        buffer << "Maximum: not defined" << std::endl;
      }

      if(_hasSum){
        buffer << "Sum: " << sum << std::endl;
      }else{
        buffer << "Sum: not defined" << std::endl;
      }
      return buffer.str();
    }
  };

  class IntegerColumnStatisticsImpl: public IntegerColumnStatistics {
  private:
    bool _hasMinimum;
    bool _hasMaximum;
    bool _hasSum;
    uint64_t valueCount;
    int64_t minimum;
    int64_t maximum;
    int64_t sum;

  public:
    IntegerColumnStatisticsImpl(const proto::ColumnStatistics& stats);
    virtual ~IntegerColumnStatisticsImpl();

    bool hasMinimum() const override {
      return _hasMinimum;
    }

    bool hasMaximum() const override {
      return _hasMaximum;
    }

    bool hasSum() const override {
      return _hasSum;
    }

    uint64_t getNumberOfValues() const override {
      return valueCount;
    }

    int64_t getMinimum() const override {
      if(_hasMinimum){
        return minimum;
      }else{
        throw ParseError("Minimum is not defined.");
      }
    }

    int64_t getMaximum() const override {
      if(_hasMaximum){
        return maximum;
      }else{
        throw ParseError("Maximum is not defined.");
      }
    }

    int64_t getSum() const override {
      if(_hasSum){
        return sum;
      }else{
        throw ParseError("Sum is not defined.");
      }
    }

    std::string toString() const override {
      std::ostringstream buffer;
      buffer << "Data type: Integer" << std::endl
          << "Values: " << valueCount << std::endl;
      if(_hasMinimum){
        buffer << "Minimum: " << minimum << std::endl;
      }else{
        buffer << "Minimum: not defined" << std::endl;
      }

      if(_hasMaximum){
        buffer << "Maximum: " << maximum << std::endl;
      }else{
        buffer << "Maximum: not defined" << std::endl;
      }

      if(_hasSum){
        buffer << "Sum: " << sum << std::endl;
      }else{
        buffer << "Sum: not defined" << std::endl;
      }
      return buffer.str();
    }
  };

  class StringColumnStatisticsImpl: public StringColumnStatistics {
  private:
    bool _hasMinimum;
    bool _hasMaximum;
    bool _hasTotalLength;
    uint64_t valueCount;
    std::string minimum;
    std::string maximum;
    uint64_t totalLength;

  public:
    StringColumnStatisticsImpl(const proto::ColumnStatistics& stats, bool correctStats);
    virtual ~StringColumnStatisticsImpl();

    bool hasMinimum() const override {
      return _hasMinimum;
    }

    bool hasMaximum() const override {
      return _hasMaximum;
    }

    bool hasTotalLength() const override {
      return _hasTotalLength;
    }

    uint64_t getNumberOfValues() const override {
      return valueCount;
    }

    std::string getMinimum() const override {
      if(_hasMinimum){
        return minimum;
      }else{
        throw ParseError("Minimum is not defined.");
      }
    }

    std::string getMaximum() const override {
      if(_hasMaximum){
        return maximum;
      }else{
        throw ParseError("Maximum is not defined.");
      }
    }

    uint64_t getTotalLength() const override {
      if(_hasTotalLength){
        return totalLength;
      }else{
        throw ParseError("Total length is not defined.");
      }
    }

    std::string toString() const override {
      std::ostringstream buffer;
      buffer << "Data type: String" << std::endl
          << "Values: " << valueCount << std::endl;
      if(_hasMinimum){
        buffer << "Minimum: " << minimum << std::endl;
      }else{
        buffer << "Minimum is not defined" << std::endl;
      }

      if(_hasMaximum){
        buffer << "Maximum: " << maximum << std::endl;
      }else{
        buffer << "Maximum is not defined" << std::endl;
      }

      if(_hasTotalLength){
        buffer << "Total length: " << totalLength << std::endl;
      }else{
        buffer << "Total length is not defined" << std::endl;
      }
      return buffer.str();
    }
  };

  class TimestampColumnStatisticsImpl: public TimestampColumnStatistics {
  private:
    bool _hasMinimum;
    bool _hasMaximum;
    uint64_t valueCount;
    int64_t minimum;
    int64_t maximum;

  public:
    TimestampColumnStatisticsImpl(const proto::ColumnStatistics& stats, bool correctStats);
    virtual ~TimestampColumnStatisticsImpl();

    bool hasMinimum() const override {
      return _hasMinimum;
    }

    bool hasMaximum() const override {
      return _hasMaximum;
    }

    uint64_t getNumberOfValues() const override {
      return valueCount;
    }

    int64_t getMinimum() const override {
      if(_hasMinimum){
        return minimum;
      }else{
        throw ParseError("Minimum is not defined.");
      }
    }

    int64_t getMaximum() const override {
      if(_hasMaximum){
        return maximum;
      }else{
        throw ParseError("Maximum is not defined.");
      }
    }

    std::string toString() const override {
      std::ostringstream buffer;
      buffer << "Data type: Timestamp" << std::endl
          << "Values: " << valueCount << std::endl;
      if(_hasMinimum){
        buffer << "Minimum: " << minimum << std::endl;
      }else{
        buffer << "Minimum is not defined" << std::endl;
      }

      if(_hasMaximum){
        buffer << "Maximum: " << maximum << std::endl;
      }else{
        buffer << "Maximum is not defined" << std::endl;
      }
      return buffer.str();
    }
  };

  class StripeInformationImpl : public StripeInformation {
    unsigned long offset;
    unsigned long indexLength;
    unsigned long dataLength;
    unsigned long footerLength;
    unsigned long numRows;

  public:

    StripeInformationImpl(unsigned long _offset,
                          unsigned long _indexLength,
                          unsigned long _dataLength,
                          unsigned long _footerLength,
                          unsigned long _numRows) :
      offset(_offset),
      indexLength(_indexLength),
      dataLength(_dataLength),
      footerLength(_footerLength),
      numRows(_numRows)
    {}

    virtual ~StripeInformationImpl();

    unsigned long getOffset() const override {
      return offset;
    }

    unsigned long getLength() const override {
      return indexLength + dataLength + footerLength;
    }
    unsigned long getIndexLength() const override {
      return indexLength;
    }

    unsigned long getDataLength()const override {
      return dataLength;
    }

    unsigned long getFooterLength() const override {
      return footerLength;
    }

    unsigned long getNumberOfRows() const override {
      return numRows;
    }
  };

  ColumnStatistics* convertColumnStatistics(const proto::ColumnStatistics& s, bool correctStats) {
    if (s.has_intstatistics()) {
      return new IntegerColumnStatisticsImpl(s);
    } else if (s.has_doublestatistics()) {
      return new DoubleColumnStatisticsImpl(s);
    } else if (s.has_stringstatistics()) {
      return new StringColumnStatisticsImpl(s, correctStats);
    } else if (s.has_bucketstatistics()) {
      return new BooleanColumnStatisticsImpl(s, correctStats);
    } else if (s.has_decimalstatistics()) {
      return new DecimalColumnStatisticsImpl(s, correctStats);
    } else if (s.has_timestampstatistics()) {
      return new TimestampColumnStatisticsImpl(s, correctStats);
    } else if (s.has_datestatistics()) {
      return new DateColumnStatisticsImpl(s, correctStats);
    } else if (s.has_binarystatistics()) {
      return new BinaryColumnStatisticsImpl(s, correctStats);
    } else {
      return new ColumnStatisticsImpl(s);
    }
  }

  Statistics::~Statistics() {
    // PASS
  }

  class StatisticsImpl: public Statistics {
  private:
    std::list<ColumnStatistics*> colStats;

    // DELIBERATELY NOT IMPLEMENTED
    StatisticsImpl(const StatisticsImpl&);
    StatisticsImpl& operator=(const StatisticsImpl&);

  public:
    StatisticsImpl(const proto::StripeStatistics& stripeStats, bool correctStats) {
      for(int i = 0; i < stripeStats.colstats_size(); i++) {
        colStats.push_back(convertColumnStatistics
                           (stripeStats.colstats(i), correctStats));
      }
    }

    StatisticsImpl(const proto::Footer& footer, bool correctStats) {
      for(int i = 0; i < footer.statistics_size(); i++) {
        colStats.push_back(convertColumnStatistics
                           (footer.statistics(i), correctStats));
      }
    }

    virtual const ColumnStatistics* getColumnStatistics(uint32_t columnId
							) const {
      std::list<ColumnStatistics*>::const_iterator it = colStats.begin();
      std::advance(it, static_cast<long>(columnId));
      return *it;
    }

    virtual ~StatisticsImpl();

    uint32_t getNumberOfColumns() const override {
      return static_cast<uint32_t>(colStats.size());
    }
  };

  StatisticsImpl::~StatisticsImpl() {
    for(std::list<ColumnStatistics*>::iterator ptr = colStats.begin();
	ptr != colStats.end();
	++ptr) {
      delete *ptr;
    }
  }

  Reader::~Reader() {
    // PASS
  }

  StripeInformationImpl::~StripeInformationImpl() {
    // PASS
  }

  static const unsigned long DIRECTORY_SIZE_GUESS = 16 * 1024;

  class ReaderImpl : public Reader {
  private:
    // inputs
    std::unique_ptr<InputStream> stream;
    ReaderOptions options;
    std::vector<bool> selectedColumns;

    // postscript
    proto::PostScript postscript;
    unsigned long blockSize;
    CompressionKind compression;
    unsigned long postscriptLength;

    // footer
    proto::Footer footer;
//    std::vector<unsigned long> firstRowOfStripe;
    std::unique_ptr<DataBuffer<uint64_t> > firstRowOfStripe;
    unsigned long numberOfStripes;
    std::unique_ptr<Type> schema;

    // metadata
    bool isMetadataLoaded;
    proto::Metadata metadata;
    unsigned long numberOfStripeStatistics;

    // reading state
    uint64_t previousRow;
    uint64_t currentStripe;
    uint64_t lastStripe;
    uint64_t currentRowInStripe;
    uint64_t rowsInCurrentStripe;
    proto::StripeInformation currentStripeInfo;
    proto::StripeFooter currentStripeFooter;
    std::unique_ptr<ColumnReader> reader;

    // custom memory pool
    MemoryPool* memoryPool;

    // internal methods
    void readPostscript(char * buffer, unsigned long length);
    void readFooter(char *buffer, unsigned long readSize,
                    unsigned long fileLength);
    proto::StripeFooter getStripeFooter(const proto::StripeInformation& info);
    void readMetadata(char *buffer, unsigned long length,
                      unsigned long fileLength);
    void startNextStripe();
    void ensureOrcFooter(char* buffer, unsigned long length);
    void checkOrcVersion();
    void selectTypeParent(size_t columnId);
    void selectTypeChildren(size_t columnId);
    std::unique_ptr<ColumnVectorBatch> createRowBatch(const Type& type,
                                                      uint64_t capacity
                                                      ) const;

  public:
    /**
     * Constructor that lets the user specify additional options.
     * @param stream the stream to read from
     * @param options options for reading
     */
    ReaderImpl(std::unique_ptr<InputStream> stream,
               const ReaderOptions& options,
               MemoryPool* pool);

    const ReaderOptions& getReaderOptions() const;
    CompressionKind getCompression() const override;

    unsigned long getNumberOfRows() const override;

    unsigned long getRowIndexStride() const override;

    const std::string& getStreamName() const override;

    std::list<std::string> getMetadataKeys() const override;

    std::string getMetadataValue(const std::string& key) const override;

    bool hasMetadataValue(const std::string& key) const override;

    unsigned long getCompressionSize() const override;

    unsigned long getNumberOfStripes() const override;

    std::unique_ptr<StripeInformation> getStripe(unsigned long
                                                 ) const override;

    unsigned long getNumberOfStripeStatistics() const override;

    std::unique_ptr<Statistics>
    getStripeStatistics(unsigned long stripeIndex) const override;


    unsigned long getContentLength() const override;

    std::unique_ptr<Statistics> getStatistics() const override;

    std::unique_ptr<ColumnStatistics> getColumnStatistics(uint32_t columnId
							  ) const override;

    const Type& getType() const override;

    const std::vector<bool> getSelectedColumns() const override;

    std::unique_ptr<ColumnVectorBatch> createRowBatch(unsigned long size
                                                      ) const override;

    bool next(ColumnVectorBatch& data) override;

    unsigned long getRowNumber() const override;

    void seekToRow(unsigned long rowNumber) override;

    MemoryPool* getMemoryPool() const ;
    
    bool hasCorrectStatistics() const override;

    virtual uint64_t memoryEstimate(int stripeIx = -1) override;
  };

  InputStream::~InputStream() {
    // PASS
  };

  ReaderImpl::ReaderImpl(std::unique_ptr<InputStream> input,
                           const ReaderOptions& opts,
                           MemoryPool* pool):
                               stream(std::move(input)), options(opts), memoryPool(pool) {
    isMetadataLoaded = false;
    // figure out the size of the file using the option or filesystem
    unsigned long size = std::min(options.getTailLocation(),
                                  static_cast<unsigned long>
                                  (stream->getLength()));

    //read last bytes into buffer to get PostScript
    unsigned long readSize = std::min(size, DIRECTORY_SIZE_GUESS);

    if (readSize < 1) {
      throw ParseError("File size too small");
    }

    DataBuffer<char> buffer(readSize, memoryPool);
    stream->read(buffer.data(), size - readSize, readSize);
    readPostscript(buffer.data(), readSize);
    readFooter(buffer.data(), readSize, size);
    
    // read metadata
    unsigned long position = size - 1 - postscriptLength
        - postscript.footerlength() - postscript.metadatalength();
    buffer.resize(postscript.metadatalength());
    stream->read(buffer.data(), position, postscript.metadatalength());

    readMetadata(buffer.data(), postscript.metadatalength(), size);

    currentStripe = static_cast<uint64_t>(footer.stripes_size());
    lastStripe = 0;
    currentRowInStripe = 0;
    unsigned long rowTotal = 0;

    // firstRowOfStripe.resize(static_cast<size_t>(footer.stripes_size()));
    firstRowOfStripe.reset(new DataBuffer<uint64_t>(
        static_cast<size_t>(footer.stripes_size()), memoryPool));

    for(size_t i=0; i < static_cast<size_t>(footer.stripes_size()); ++i) {
      (*firstRowOfStripe)[i] = rowTotal;
      proto::StripeInformation stripeInfo = footer.stripes(static_cast<int>(i));
      rowTotal += stripeInfo.numberofrows();
      bool isStripeInRange = stripeInfo.offset() >= opts.getOffset() &&
        stripeInfo.offset() < opts.getOffset() + opts.getLength();
      if (isStripeInRange) {
        if (i < currentStripe) {
          currentStripe = i;
        }
        if (i > lastStripe) {
          lastStripe = i;
        }
      }
    }

    schema = convertType(footer.types(0), footer);
    schema->assignIds(0);
    previousRow = (std::numeric_limits<uint64_t>::max)();

    selectedColumns.assign(static_cast<size_t>(footer.types_size()), false);

    const std::list<int>& included = options.getInclude();
    for(std::list<int>::const_iterator columnId = included.begin();
        columnId != included.end(); ++columnId) {
      if (*columnId <= static_cast<int>(schema->getSubtypeCount())) {
        selectTypeParent(static_cast<size_t>(*columnId));
        selectTypeChildren(static_cast<size_t>(*columnId));
      }
    }
  }

  const ReaderOptions& ReaderImpl::getReaderOptions() const {
    return options;
  }

  CompressionKind ReaderImpl::getCompression() const {
    return compression;
  }

  unsigned long ReaderImpl::getCompressionSize() const {
    return blockSize;
  }

  unsigned long ReaderImpl::getNumberOfStripes() const {
    return numberOfStripes;
  }

  unsigned long ReaderImpl::getNumberOfStripeStatistics() const {
    return numberOfStripeStatistics;
  }

  std::unique_ptr<StripeInformation>
  ReaderImpl::getStripe(unsigned long stripeIndex) const {
    if (stripeIndex > getNumberOfStripes()) {
      throw std::logic_error("stripe index out of range");
    }
    proto::StripeInformation stripeInfo =
      footer.stripes(static_cast<int>(stripeIndex));

    return std::unique_ptr<StripeInformation>
      (new StripeInformationImpl
       (stripeInfo.offset(),
        stripeInfo.indexlength(),
        stripeInfo.datalength(),
        stripeInfo.footerlength(),
        stripeInfo.numberofrows()));
  }

  unsigned long ReaderImpl::getNumberOfRows() const {
    return footer.numberofrows();
  }

  unsigned long ReaderImpl::getContentLength() const {
    return footer.contentlength();
  }

  unsigned long ReaderImpl::getRowIndexStride() const {
    return footer.rowindexstride();
  }

  const std::string& ReaderImpl::getStreamName() const {
    return stream->getName();
  }

  std::list<std::string> ReaderImpl::getMetadataKeys() const {
    std::list<std::string> result;
    for(int i=0; i < footer.metadata_size(); ++i) {
      result.push_back(footer.metadata(i).name());
    }
    return result;
  }

  std::string ReaderImpl::getMetadataValue(const std::string& key) const {
    for(int i=0; i < footer.metadata_size(); ++i) {
      if (footer.metadata(i).name() == key) {
        return footer.metadata(i).value();
      }
    }
    throw std::range_error("key not found");
  }

  bool ReaderImpl::hasMetadataValue(const std::string& key) const {
    for(int i=0; i < footer.metadata_size(); ++i) {
      if (footer.metadata(i).name() == key) {
        return true;
      }
    }
    return false;
  }

  void ReaderImpl::selectTypeParent(size_t columnId) {
    for(size_t parent=0; parent < columnId; ++parent) {
      const proto::Type& parentType = footer.types(static_cast<int>(parent));
      for(int idx=0; idx < parentType.subtypes_size(); ++idx) {
        unsigned int child = parentType.subtypes(idx);
        if (child == columnId) {
          if (!selectedColumns[parent]) {
            selectedColumns[parent] = true;
            selectTypeParent(parent);
            return;
          }
        }
      }
    }
  }

  void ReaderImpl::selectTypeChildren(size_t columnId) {
    if (!selectedColumns[columnId]) {
      selectedColumns[columnId] = true;
      const proto::Type& parentType = footer.types(static_cast<int>(columnId));
      for(int idx=0; idx < parentType.subtypes_size(); ++idx) {
        unsigned int child = parentType.subtypes(idx);
        selectTypeChildren(child);
      }
    }
  }

  void ReaderImpl::ensureOrcFooter(char *buffer, unsigned long readSize) {

    const std::string MAGIC("ORC");

    unsigned long len = MAGIC.length();
    if (postscriptLength < len + 1) {
      throw ParseError("Invalid postscript length");
    }

    // Look for the magic string at the end of the postscript.
    if (memcmp(buffer+readSize-1-len, MAGIC.c_str(), len) != 0) {
      // if there is no magic string at the end, check the beginning of the file
      DataBuffer<char> frontBuffer(len, memoryPool);
      stream->read(frontBuffer.data(), 0, len);
      if (memcmp(frontBuffer.data(), MAGIC.c_str(), len) != 0) {
        throw ParseError("Not an ORC file");
      }
    }
  }

  const std::vector<bool> ReaderImpl::getSelectedColumns() const {
    return selectedColumns;
  }

  const Type& ReaderImpl::getType() const {
    return *(schema.get());
  }

  unsigned long ReaderImpl::getRowNumber() const {
    return previousRow;
  }

  std::unique_ptr<Statistics> ReaderImpl::getStatistics() const {
    return std::unique_ptr<Statistics>(new StatisticsImpl(footer, hasCorrectStatistics()));
  }

  std::unique_ptr<ColumnStatistics>
  ReaderImpl::getColumnStatistics(uint32_t index) const {
    if (index >= static_cast<unsigned int>(footer.statistics_size())) {
      throw std::logic_error("column index out of range");
    }
    proto::ColumnStatistics col = footer.statistics(static_cast<int>(index));
    return std::unique_ptr<ColumnStatistics> (convertColumnStatistics
                                              (col, hasCorrectStatistics()));
  }

  std::unique_ptr<Statistics>
  ReaderImpl::getStripeStatistics(unsigned long stripeIndex) const {
    if(numberOfStripeStatistics == 0){
      throw std::logic_error("No stripe statistics in file");
    }
    if(stripeIndex >= numberOfStripeStatistics) {
      throw std::logic_error("stripe index out of range");
    }
    return std::unique_ptr<Statistics>
      (new StatisticsImpl(metadata.stripestats
                          (static_cast<int>(stripeIndex)), hasCorrectStatistics()));
  }


  void ReaderImpl::seekToRow(unsigned long) {
    throw NotImplementedYet("seekToRow");
  }

  MemoryPool* ReaderImpl::getMemoryPool() const {
    return memoryPool;
  }

  bool ReaderImpl::hasCorrectStatistics() const {
    return postscript.has_writerversion() && postscript.writerversion();
  }

  void ReaderImpl::readPostscript(char *buffer, unsigned long readSize) {
    postscriptLength = buffer[readSize - 1] & 0xff;

    ensureOrcFooter(buffer, readSize);

    if (!postscript.ParseFromArray(buffer+readSize-1-postscriptLength,
                                   static_cast<int>(postscriptLength))) {
      throw ParseError("Failed to parse the postscript");
    }
    if (postscript.has_compressionblocksize()) {
      blockSize = postscript.compressionblocksize();
    } else {
      blockSize = 256 * 1024;
    }

    checkOrcVersion();

    //check compression codec
    compression = static_cast<CompressionKind>(postscript.compression());
  }

  void ReaderImpl::readFooter(char* buffer, unsigned long readSize,
                              unsigned long fileLength) {
    unsigned long footerSize = postscript.footerlength();
    unsigned long tailSize = 1 + postscriptLength + footerSize;

    char* pBuffer = buffer + (readSize - tailSize);
    std::unique_ptr<DataBuffer<char> > extraBuffer;

    if (tailSize > readSize) {
      // Read the rest of the footer
      unsigned long extra = tailSize - readSize;

      extraBuffer.reset(new DataBuffer<char>(footerSize, memoryPool));
      stream->read(extraBuffer->data(), fileLength - tailSize, extra);
      memcpy(extraBuffer->data()+extra,buffer,readSize-1-postscriptLength);
      pBuffer = extraBuffer->data();
    }
    std::unique_ptr<SeekableInputStream> pbStream =
      createDecompressor(compression,
                         std::unique_ptr<SeekableInputStream>
                         (new SeekableArrayInputStream(pBuffer, footerSize, memoryPool)),
                         blockSize);
    // TODO: do not SeekableArrayInputStream, rather use an array
    //    if (!footer.ParseFromArray(buffer+readSize-tailSize, footerSize)) {
    if (!footer.ParseFromZeroCopyStream(pbStream.get())) {
      throw ParseError("Failed to parse the footer");
    }

    numberOfStripes = static_cast<unsigned long>(footer.stripes_size());
  }

  proto::StripeFooter ReaderImpl::getStripeFooter
  (const proto::StripeInformation& info) {
    unsigned long footerStart = info.offset() + info.indexlength() +
      info.datalength();
    unsigned long footerLength = info.footerlength();
    std::unique_ptr<SeekableInputStream> pbStream =
      createDecompressor(compression,
                         std::unique_ptr<SeekableInputStream>
                         (new SeekableFileInputStream(
                            stream.get(),
                            footerStart,
                            footerLength,
                            memoryPool,
                            std::max(static_cast<long>(blockSize),
                                static_cast<long>(options.getFileBlockSize()))
                         )),
                         blockSize);
    proto::StripeFooter result;
    if (!result.ParseFromZeroCopyStream(pbStream.get())) {
      throw ParseError(std::string("bad StripeFooter from ") +
                       pbStream->getName());
    }
    return result;
  }

  void ReaderImpl::readMetadata(char* buffer, unsigned long readSize, unsigned long)
  {
    unsigned long metadataSize = postscript.metadatalength();

    //check if extra bytes need to be read
    unsigned long tailSize = metadataSize;
    if (tailSize > readSize) {
      throw NotImplementedYet("need more file metadata data.");
    }
    std::unique_ptr<SeekableInputStream> pbStream =
      createDecompressor(compression,
                         std::unique_ptr<SeekableInputStream>
                         (new SeekableArrayInputStream(buffer+(readSize - tailSize),
                                                       metadataSize,memoryPool)),
                         blockSize);

    if (!metadata.ParseFromZeroCopyStream(pbStream.get())) {
      throw ParseError("bad metadata parse");
    }

    numberOfStripeStatistics = static_cast<unsigned long>(metadata.stripestats_size());
  }

  uint64_t ReaderImpl::memoryEstimate(int stripeIx) {
    uint64_t memory = 0;

    /* TODO: ReaderImpl currently does not read stripe indices.
     * When we add index support, memory per stripe is:
     * stripe = footer.stripes(i).datalength()+footer.stripes(i).indexlength();
     */
    uint64_t stripe ;
    if (stripeIx >= 0 && stripeIx < footer.stripes_size()) {
      stripe = footer.stripes(stripeIx).datalength();
      if (memory < stripe) {
        memory = stripe;
      }
    } else {
      for (int i=0; i < footer.stripes_size(); i++) {
        stripe = footer.stripes(i).datalength();
        if (memory < stripe) {
          memory = stripe;
        }
      }
    }

    // If no string columns are selected, we can potentially tighten the estimate.
    bool hasStringColumn = false;
    uint64_t nSelectedColumns = 0;
    for (unsigned int i=0; i < schema->getSubtypeCount(); i++) {
      if (selectedColumns[i+1]) {
        nSelectedColumns++ ;
        switch (static_cast<unsigned int>(schema->getSubtype(i).getKind())) {
          case CHAR:
          case STRING:
          case VARCHAR:
          case BINARY: {
            hasStringColumn = true;
            break;
          }
          default: {
            break;
          }
        }
      }
    }
    if (!hasStringColumn && memory > nSelectedColumns * options.getFileBlockSize()) {
      memory = nSelectedColumns * options.getFileBlockSize() ;
    }

    // Do we need even more memory to read the footer or the metadata?
    if (memory < postscript.footerlength()) {
      memory =  postscript.footerlength();
    }
    if (memory < postscript.metadatalength()) {
      memory =  postscript.metadatalength();
    }

    // Account for firstRowOfStripe.
    memory += firstRowOfStripe->capacity() * sizeof(uint64_t);

    // The decompressor needs a buffer for each stream
    if (compression != CompressionKind_NONE) {
      for (unsigned int i=0; i < schema->getSubtypeCount(); i++) {
        if (selectedColumns[i+1]) {
          switch (static_cast<unsigned int>(schema->getSubtype(i).getKind())) {
          case STRUCT: {
            memory += blockSize ;
            break;
          }
          case INT:
          case LONG:
          case SHORT:
          case FLOAT:
          case DOUBLE:
          case BOOLEAN:
          case BYTE:
          case DATE:
          case LIST:
          case MAP:
          case UNION: {
            memory += 2*blockSize ;
            break;
          }
          case BINARY:
          case DECIMAL:
          case TIMESTAMP: {
            memory += 3*blockSize ;
            break;
          }
          case CHAR:
          case STRING:
          case VARCHAR: {
            memory += 4*blockSize ;
            break;
          }
          default:
            break;
          }
        }
      }
    }

    return memory ;
  }


  class StripeStreamsImpl: public StripeStreams {
  private:
    const ReaderImpl& reader;
    const proto::StripeFooter& footer;
    const unsigned long stripeStart;
    InputStream& input;

  public:
    StripeStreamsImpl(const ReaderImpl& reader,
                      const proto::StripeFooter& footer,
                      unsigned long stripeStart,
                      InputStream& input);

    virtual ~StripeStreamsImpl();

    virtual const ReaderOptions& getReaderOptions() const override;

    virtual const std::vector<bool> getSelectedColumns() const override;

    virtual proto::ColumnEncoding getEncoding(int columnId) const override;

    virtual std::unique_ptr<SeekableInputStream>
    getStream(int columnId,
              proto::Stream_Kind kind) const override;
  };

  StripeStreamsImpl::StripeStreamsImpl(const ReaderImpl& _reader,
                                       const proto::StripeFooter& _footer,
                                       unsigned long _stripeStart,
                                       InputStream& _input
                                       ): reader(_reader),
                                          footer(_footer),
                                          stripeStart(_stripeStart),
                                          input(_input) {
    // PASS
  }

  StripeStreamsImpl::~StripeStreamsImpl() {
    // PASS
  }

  const ReaderOptions& StripeStreamsImpl::getReaderOptions() const {
    return reader.getReaderOptions();
  }

  const std::vector<bool> StripeStreamsImpl::getSelectedColumns() const {
    return reader.getSelectedColumns();
  }

  proto::ColumnEncoding StripeStreamsImpl::getEncoding(int columnId) const {
    return footer.columns(columnId);
  }

  std::unique_ptr<SeekableInputStream>
  StripeStreamsImpl::getStream(int columnId,
                               proto::Stream_Kind kind) const {
    unsigned long offset = stripeStart;
    for(int i = 0; i < footer.streams_size(); ++i) {
      const proto::Stream& stream = footer.streams(i);
      if (stream.kind() == kind &&
          stream.column() == static_cast<unsigned int>(columnId)) {
        return createDecompressor(
                  reader.getCompression(),
                  std::unique_ptr<SeekableInputStream>
                  (new SeekableFileInputStream
                   (&input,
                    offset,
                    stream.length(),
                    static_cast<MemoryPool*>(reader.getMemoryPool()),
                    std::max(static_cast<long>(reader.getCompressionSize()),
                        static_cast<long>(
                            reader.getReaderOptions().getFileBlockSize())
                            )
                   )),
                  reader.getCompressionSize());
      }
      offset += stream.length();
    }
    return std::unique_ptr<SeekableInputStream>();
  }

  void ReaderImpl::startNextStripe() {
    currentStripeInfo = footer.stripes(static_cast<int>(currentStripe));
    currentStripeFooter = getStripeFooter(currentStripeInfo);
    rowsInCurrentStripe = currentStripeInfo.numberofrows();
    StripeStreamsImpl stripeStreams(*this, currentStripeFooter,
                                    currentStripeInfo.offset(),
                                    *(stream.get()));

    // Do not remove the following line!
    // It halves the amount of memory used by column readers.
    reader.reset();
    reader = buildReader(*(schema.get()), stripeStreams, memoryPool);
  }

  void ReaderImpl::checkOrcVersion() {
    // TODO
  }

  bool ReaderImpl::next(ColumnVectorBatch& data) {
    if (currentStripe > lastStripe) {
      data.numElements = 0;
      previousRow = (*firstRowOfStripe)[lastStripe] +
        footer.stripes(static_cast<int>(lastStripe)).numberofrows();
      return false;
    }
    if (currentRowInStripe == 0) {
      startNextStripe();
    }
    uint64_t rowsToRead =
      std::min(static_cast<uint64_t>(data.capacity),
               rowsInCurrentStripe - currentRowInStripe);
    data.numElements = rowsToRead;
    reader->next(data, rowsToRead, 0);
    // update row number
    previousRow = (*firstRowOfStripe)[currentStripe] + currentRowInStripe;
    currentRowInStripe += rowsToRead;
    if (currentRowInStripe >= rowsInCurrentStripe) {
      currentStripe += 1;
      currentRowInStripe = 0;
    }
    return rowsToRead != 0;
  }

  std::unique_ptr<ColumnVectorBatch> ReaderImpl::createRowBatch
  (const Type& type, uint64_t capacity) const {
    ColumnVectorBatch* result = nullptr;
    const Type* subtype;
    switch (static_cast<int>(type.getKind())) {
    case BOOLEAN:
    case BYTE:
    case SHORT:
    case INT:
    case LONG:
    case TIMESTAMP:
    case DATE:
      result = new LongVectorBatch(capacity, memoryPool);
      break;
    case FLOAT:
    case DOUBLE:
      result = new DoubleVectorBatch(capacity, memoryPool);
      break;
    case STRING:
    case BINARY:
    case CHAR:
    case VARCHAR:
      result = new StringVectorBatch(capacity, memoryPool);
      break;
    case STRUCT:
      result = new StructVectorBatch(capacity, memoryPool);
      for(unsigned int i=0; i < type.getSubtypeCount(); ++i) {
        subtype = &(type.getSubtype(i));
        if (selectedColumns[static_cast<size_t>(subtype->getColumnId())]) {
          dynamic_cast<StructVectorBatch*>(result)->fields.push_back(
            (createRowBatch(*subtype, capacity).release()));
        }
      }
      break;
    case LIST:
      result = new ListVectorBatch(capacity, memoryPool);
      subtype = &(type.getSubtype(0));
      if (selectedColumns[static_cast<size_t>(subtype->getColumnId())]) {
        dynamic_cast<ListVectorBatch*>(result)->elements =
          createRowBatch(*subtype, capacity);
      }
      break;
    case MAP:
      result = new MapVectorBatch(capacity, memoryPool);
      subtype = &(type.getSubtype(0));
      if (selectedColumns[static_cast<size_t>(subtype->getColumnId())]) {
        dynamic_cast<MapVectorBatch*>(result)->keys =
          createRowBatch(*subtype, capacity);
      }
      subtype = &(type.getSubtype(1));
      if (selectedColumns[static_cast<size_t>(subtype->getColumnId())]) {
        dynamic_cast<MapVectorBatch*>(result)->elements =
          createRowBatch(*subtype, capacity);
      }
      break;
    case DECIMAL:
      if (type.getPrecision() == 0 || type.getPrecision() > 18) {
        result = new Decimal128VectorBatch(capacity, memoryPool);
      } else {
        result = new Decimal64VectorBatch(capacity, memoryPool);
      }
      break;
    case UNION:
    default:
      throw NotImplementedYet("not supported yet");
    }
    return std::unique_ptr<ColumnVectorBatch>(result);
  }

  std::unique_ptr<ColumnVectorBatch> ReaderImpl::createRowBatch
                                              (unsigned long capacity) const {
    return createRowBatch(*(schema.get()), capacity);
  }

  std::unique_ptr<Reader> createReader(std::unique_ptr<InputStream> stream,
                                       const ReaderOptions& options,
                                       MemoryPool* pool) {
    return std::unique_ptr<Reader>(new ReaderImpl(std::move(stream), options, pool));
  }

  ColumnStatistics::~ColumnStatistics() {
    // PASS
  }

  BinaryColumnStatistics::~BinaryColumnStatistics() {
    // PASS
  }

  BooleanColumnStatistics::~BooleanColumnStatistics() {
    // PASS
  }

  DateColumnStatistics::~DateColumnStatistics() {
    // PASS
  }

  DecimalColumnStatistics::~DecimalColumnStatistics() {
    // PASS
  }

  DoubleColumnStatistics::~DoubleColumnStatistics() {
    // PASS
  }

  IntegerColumnStatistics::~IntegerColumnStatistics() {
    // PASS
  }

  StringColumnStatistics::~StringColumnStatistics() {
    // PASS
  }

  TimestampColumnStatistics::~TimestampColumnStatistics() {
    // PASS
  }

  ColumnStatisticsImpl::~ColumnStatisticsImpl() {
    // PASS
  }

  BinaryColumnStatisticsImpl::~BinaryColumnStatisticsImpl() {
    // PASS
  }

  BooleanColumnStatisticsImpl::~BooleanColumnStatisticsImpl() {
    // PASS
  }

  DateColumnStatisticsImpl::~DateColumnStatisticsImpl() {
    // PASS
  }

  DecimalColumnStatisticsImpl::~DecimalColumnStatisticsImpl() {
    // PASS
  }

  DoubleColumnStatisticsImpl::~DoubleColumnStatisticsImpl() {
    // PASS
  }

  IntegerColumnStatisticsImpl::~IntegerColumnStatisticsImpl() {
    // PASS
  }

  StringColumnStatisticsImpl::~StringColumnStatisticsImpl() {
    // PASS
  }

  TimestampColumnStatisticsImpl::~TimestampColumnStatisticsImpl() {
    // PASS
  }

  ColumnStatisticsImpl::ColumnStatisticsImpl
  (const proto::ColumnStatistics& pb) {
    valueCount = pb.numberofvalues();
  }

  BinaryColumnStatisticsImpl::BinaryColumnStatisticsImpl
  (const proto::ColumnStatistics& pb, bool correctStats){
    valueCount = pb.numberofvalues();
    if (!pb.has_binarystatistics() || !correctStats) {
      _hasTotalLength = false;
    }else{
      _hasTotalLength = pb.binarystatistics().has_sum();
      totalLength = static_cast<uint64_t>(pb.binarystatistics().sum());
    }
  }

  BooleanColumnStatisticsImpl::BooleanColumnStatisticsImpl
  (const proto::ColumnStatistics& pb, bool correctStats){
    valueCount = pb.numberofvalues();
    if (!pb.has_bucketstatistics() || !correctStats) {
      _hasCount = false;
    }else{
      _hasCount = true;
      trueCount = pb.bucketstatistics().count(0);
    }
  }

  DateColumnStatisticsImpl::DateColumnStatisticsImpl
  (const proto::ColumnStatistics& pb, bool correctStats){
    valueCount = pb.numberofvalues();
    if (!pb.has_datestatistics() || !correctStats) {
      _hasMinimum = false;
      _hasMaximum = false;
    }else{
        _hasMinimum = pb.datestatistics().has_minimum();
        _hasMaximum = pb.datestatistics().has_maximum();
        minimum = pb.datestatistics().minimum();
        maximum = pb.datestatistics().maximum();
    }
  }

  DecimalColumnStatisticsImpl::DecimalColumnStatisticsImpl
  (const proto::ColumnStatistics& pb, bool correctStats){
    valueCount = pb.numberofvalues();
    if (!pb.has_decimalstatistics() || !correctStats) {
      _hasMinimum = false;
      _hasMaximum = false;
      _hasSum = false;
    }else{
      const proto::DecimalStatistics& stats = pb.decimalstatistics();
      _hasMinimum = stats.has_minimum();
      _hasMaximum = stats.has_maximum();
      _hasSum = stats.has_sum();

      minimum = stats.minimum();
      maximum = stats.maximum();
      sum = stats.sum();
    }
  }

  DoubleColumnStatisticsImpl::DoubleColumnStatisticsImpl
  (const proto::ColumnStatistics& pb){
    valueCount = pb.numberofvalues();
    if (!pb.has_doublestatistics()) {
      _hasMinimum = false;
      _hasMaximum = false;
      _hasSum = false;
    }else{
      const proto::DoubleStatistics& stats = pb.doublestatistics();
      _hasMinimum = stats.has_minimum();
      _hasMaximum = stats.has_maximum();
      _hasSum = stats.has_sum();

      minimum = stats.minimum();
      maximum = stats.maximum();
      sum = stats.sum();
    }
  }

  IntegerColumnStatisticsImpl::IntegerColumnStatisticsImpl
  (const proto::ColumnStatistics& pb){
    valueCount = pb.numberofvalues();
    if (!pb.has_intstatistics()) {
      _hasMinimum = false;
      _hasMaximum = false;
      _hasSum = false;
    }else{
      const proto::IntegerStatistics& stats = pb.intstatistics();
      _hasMinimum = stats.has_minimum();
      _hasMaximum = stats.has_maximum();
      _hasSum = stats.has_sum();

      minimum = stats.minimum();
      maximum = stats.maximum();
      sum = stats.sum();
    }
  }

  StringColumnStatisticsImpl::StringColumnStatisticsImpl
  (const proto::ColumnStatistics& pb, bool correctStats){
    valueCount = pb.numberofvalues();
    if (!pb.has_stringstatistics() || !correctStats) {
      _hasMinimum = false;
      _hasMaximum = false;
      _hasTotalLength = false;
    }else{
      const proto::StringStatistics& stats = pb.stringstatistics();
      _hasMinimum = stats.has_minimum();
      _hasMaximum = stats.has_maximum();
      _hasTotalLength = stats.has_sum();

      minimum = stats.minimum();
      maximum = stats.maximum();
      totalLength = static_cast<uint64_t>(stats.sum());
    }
  }

  TimestampColumnStatisticsImpl::TimestampColumnStatisticsImpl
  (const proto::ColumnStatistics& pb, bool correctStats){
    valueCount = pb.numberofvalues();
    if (!pb.has_timestampstatistics() || !correctStats) {
      _hasMinimum = false;
      _hasMaximum = false;
    }else{
      const proto::TimestampStatistics& stats = pb.timestampstatistics();
      _hasMinimum = stats.has_minimum();
      _hasMaximum = stats.has_maximum();

      minimum = stats.minimum();
      maximum = stats.maximum();
    }
  }

}// namespace
