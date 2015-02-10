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
    ReaderOptionsPrivate() {
      includedColumns.assign(1,0);
      dataStart = 0;
      dataLength = std::numeric_limits<unsigned long>::max();
      tailLocation = std::numeric_limits<unsigned long>::max();
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


  StripeInformation::~StripeInformation() {

  }

  class StripeInformationImpl : public StripeInformation {
    unsigned long offset;
    unsigned long indexLength;
    unsigned long dataLength;
    unsigned long footerLength;
    unsigned long numRows;

  public:

    StripeInformationImpl(unsigned long offset,
                          unsigned long indexLength,
                          unsigned long dataLength,
                          unsigned long footerLength,
                          unsigned long numRows) :
      offset(offset),
      indexLength(indexLength),
      dataLength(dataLength),
      footerLength(footerLength),
      numRows(numRows)
    {}

    ~StripeInformationImpl() {}

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

ColumnStatistics* convertColumnStatistics(const proto::ColumnStatistics&columnStats, orc::TypeKind colType)
{
    orc::ColumnStatisticsPrivate* colPrivateTmp = 
                new ColumnStatisticsPrivate(columnStats);
    switch(colType){
      case orc::BYTE:
      case orc::SHORT:
      case orc::INT:
      case orc::LONG:
      {
          IntegerColumnStatistics *col = new IntegerColumnStatistics(
              std::unique_ptr<ColumnStatisticsPrivate> (colPrivateTmp));
          return col;
      }
      case orc::STRING:
      case orc::CHAR:
      case orc::VARCHAR:
      {
          StringColumnStatistics *col = new StringColumnStatistics(
              std::unique_ptr<ColumnStatisticsPrivate> (colPrivateTmp));
          return col;
          
      }
      case orc::FLOAT:
      case orc::DOUBLE:
      {
          DoubleColumnStatistics *col = new DoubleColumnStatistics(
              std::unique_ptr<ColumnStatisticsPrivate> (colPrivateTmp));
          return col;
      }
      case orc::TIMESTAMP:
      {
          TimestampColumnStatistics *col = new TimestampColumnStatistics(
              std::unique_ptr<ColumnStatisticsPrivate> (colPrivateTmp));
          return col;
      }
      case orc::BINARY:
      {
          BinaryColumnStatistics *col = new BinaryColumnStatistics(
              std::unique_ptr<ColumnStatisticsPrivate> (colPrivateTmp));
          return col;
      }
      case orc::DECIMAL:
      {
          DecimalColumnStatistics *col = new DecimalColumnStatistics(
              std::unique_ptr<ColumnStatisticsPrivate> (colPrivateTmp));
          return col;
      }
      case orc::BOOLEAN:
      {
          ColumnStatistics *col = new ColumnStatistics(
              std::unique_ptr<ColumnStatisticsPrivate> (colPrivateTmp));
          return col;
      }
      default:
        throw ParseError("data type is not supported for the ORC file format");
    }
}


class StripeStatisticsImpl: public StripeStatistics {
private:
    unsigned long numberOfColStats;
    std::list<ColumnStatistics*> colStats;
public:
    StripeStatisticsImpl(proto::StripeStatistics stripeStats, const Type & schema)
    {
        for(int i = 0; i < stripeStats.colstats_size()-1; i++){
            colStats.push_back(orc::convertColumnStatistics(stripeStats.colstats(i+1),
                                                            schema.getSubtype(i).getKind()));
        }
        // stripeStats has one more column than schema. 
        numberOfColStats = stripeStats.colstats_size()-1;
    }
    std::unique_ptr<ColumnStatistics> getColumnStatisticsInStripe(unsigned long colIndex) const override
    {
        if(colIndex > numberOfColStats){
            throw std::logic_error("column index out of range");
        }
        std::list<ColumnStatistics*>::const_iterator it = colStats.begin();
        std::advance(it, colIndex);
        return std::unique_ptr<ColumnStatistics> (*it);
    }

    std::list<ColumnStatistics*> getStatisticsInStripe() const override
    {
        return colStats;
    }

    unsigned long getNumberOfColumnStatistics() const override 
    {
        return numberOfColStats;
    }
};


  Reader::~Reader() {
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
    std::vector<unsigned long> firstRowOfStripe;
    unsigned long numberOfStripes;
    std::unique_ptr<Type> schema;

    // metadata
    bool isMetadataLoaded;
    proto::Metadata metadata;

    // reading state
    uint64_t previousRow;
    uint64_t currentStripe;
    uint64_t lastStripe;
    uint64_t currentRowInStripe;
    uint64_t rowsInCurrentStripe;
    proto::StripeInformation currentStripeInfo;
    proto::StripeFooter currentStripeFooter;
    std::unique_ptr<ColumnReader> reader;

    // internal methods
    void readPostscript(char * buffer, unsigned long length);
    void readFooter(char *buffer, unsigned long length,
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
               const ReaderOptions& options);

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

    std::unique_ptr<StripeStatistics> getStripeStatistics(unsigned long stripeIndex) const override;
      

    unsigned long getContentLength() const override;

    std::list<ColumnStatistics*> getStatistics() const override;
    
    std::unique_ptr<ColumnStatistics> getColumnStatistics(unsigned long index) const override;

    const Type& getType() const override;

    const std::vector<bool> getSelectedColumns() const override;

    std::unique_ptr<ColumnVectorBatch> createRowBatch(unsigned long size
                                                      ) const override;

    bool next(ColumnVectorBatch& data) override;

    unsigned long getRowNumber() const override;

    void seekToRow(unsigned long rowNumber) override;
  };

  InputStream::~InputStream() {
    // PASS
  };


  ReaderImpl::ReaderImpl(std::unique_ptr<InputStream> input,
                         const ReaderOptions& opts
                         ): stream(std::move(input)), options(opts) {
    isMetadataLoaded = false;
    // figure out the size of the file using the option or filesystem
    unsigned long size = std::min(options.getTailLocation(),
                                  static_cast<unsigned long> (stream->getLength()));

    //read last bytes into buffer to get PostScript
    unsigned long readSize = std::min(size, DIRECTORY_SIZE_GUESS);

    if (readSize < 1) {
      throw ParseError("File size too small");
    }

    std::vector<char> buffer(readSize);
    stream->read(buffer.data(), size - readSize, readSize);
    readPostscript(buffer.data(), readSize);
    readFooter(buffer.data(), readSize, size);
    
    // read metadata
    unsigned long position = size - 1 - postscript.footerlength() - postscriptLength - postscript.metadatalength();
    buffer.resize(postscript.metadatalength());
    stream->read(buffer.data(), position, postscript.metadatalength());

    readMetadata(buffer.data(), postscript.metadatalength(), size);

    currentStripe = footer.stripes_size();
    lastStripe = 0;
    currentRowInStripe = 0;
    unsigned long rowTotal = 0;
    firstRowOfStripe.resize(static_cast<size_t>(footer.stripes_size()));
    for(size_t i=0; i < static_cast<size_t>(footer.stripes_size()); ++i) {
      firstRowOfStripe[i] = rowTotal;
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
    previousRow = (std::numeric_limits<unsigned long>::max)();

    selectedColumns.assign(static_cast<size_t>(footer.types_size()), false);

    const std::list<int>& included = options.getInclude();
    for(std::list<int>::const_iterator columnId = included.begin();
        columnId != included.end(); ++columnId) {
      if (*columnId <= (int)(schema->getSubtypeCount())) {
        selectTypeParent(static_cast<size_t>(*columnId));
        selectTypeChildren(static_cast<size_t>(*columnId));
      }
    }
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
      throw ParseError("Malformed ORC file: invalid postscript length");
    }

    // Look for the magic string at the end of the postscript.
    if (memcmp(buffer+readSize-1-postscriptLength, MAGIC.c_str(), MAGIC.length()) != 0) {
      // if there is no magic string at the end, check the beginning of the file
      std::vector<char> frontBuffer(MAGIC.length());
      stream->read(frontBuffer.data(), 0, MAGIC.length());
      if (memcmp(frontBuffer.data(), MAGIC.c_str(), MAGIC.length()) != 0) {
        throw ParseError("Malformed ORC file: invalid postscript");
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

  std::list<ColumnStatistics*> ReaderImpl::getStatistics() const {
      std::list<ColumnStatistics*> result;
      for(uint colIdx=0; colIdx < schema->getSubtypeCount(); ++colIdx) {

          // colIdx + 1 because selectedColumns size = numberOfCols + 1. 
          // same size of footer. Should skip selectedColumns.at(0)
          if(selectedColumns.at(colIdx+1)){
              orc::TypeKind colType = schema->getSubtype(colIdx).getKind();
              proto::ColumnStatistics col = footer.statistics(colIdx+1);
              result.push_back(convertColumnStatistics(col, colType));
          }
      }
      return result;
  }

// index start from 0
std::unique_ptr<ColumnStatistics> ReaderImpl::getColumnStatistics(unsigned long index) const {
    if(index >= (unsigned int)footer.statistics_size()){
        throw std::logic_error("column index out of range");
    }
    orc::TypeKind colType = schema->getSubtype(index).getKind();
    proto::ColumnStatistics col = footer.statistics(index+1);
    return std::unique_ptr<ColumnStatistics> (convertColumnStatistics(col, colType));
}


std::unique_ptr<StripeStatistics> ReaderImpl::getStripeStatistics(unsigned long stripeIndex) const {
    if(stripeIndex > (unsigned int)metadata.stripestats_size()){
        throw std::logic_error("stripe index out of range");
    }
    return std::unique_ptr<StripeStatistics> 
      (new StripeStatisticsImpl(metadata.stripestats(stripeIndex), getType()));
}


  void ReaderImpl::seekToRow(unsigned long) {
    throw NotImplementedYet("seekToRow");
  }

  void ReaderImpl::readPostscript(char *buffer, unsigned long readSize) {
    postscriptLength = buffer[readSize - 1] & 0xff;

    ensureOrcFooter(buffer, readSize);

    if (!postscript.ParseFromArray(buffer+readSize-1-postscriptLength, 
                                   static_cast<int>(postscriptLength))) {
      throw ParseError("bad postscript parse");
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
                              unsigned long) {
    unsigned long footerSize = postscript.footerlength();
    //check if extra bytes need to be read
    unsigned long tailSize = 1 + postscriptLength + footerSize;
    if (tailSize > readSize) {
      throw NotImplementedYet("need more footer data.");
    }
    std::unique_ptr<SeekableInputStream> pbStream =
      createDecompressor(compression,
                          std::unique_ptr<SeekableInputStream>
                          (new SeekableArrayInputStream(buffer +
                                                        (readSize - tailSize),
                                                        footerSize)),
                          blockSize);
    // TODO: do not SeekableArrayInputStream, rather use an array
//    if (!footer.ParseFromArray(buffer+readSize-tailSize, footerSize)) {
    if (!footer.ParseFromZeroCopyStream(pbStream.get())) {
      throw ParseError("bad footer parse");
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
                         (new SeekableFileInputStream(stream.get(), 
                                                      footerStart,
                                                      footerLength, 
                                                      static_cast<long>
                                                      (blockSize)
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
                                                        metadataSize)),
                         blockSize);
    
    if (!metadata.ParseFromZeroCopyStream(pbStream.get())) {
      throw ParseError("bad metadata parse");
    }
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
        return createDecompressor(reader.getCompression(),
                                  std::unique_ptr<SeekableInputStream>
                                  (new SeekableFileInputStream
                                   (&input,
                                    offset,
                                    stream.length(),
                                    static_cast<long>
                                    (reader.getCompressionSize()))),
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
    reader = buildReader(*(schema.get()), stripeStreams);
  }

  void ReaderImpl::checkOrcVersion() {
    // TODO
  }

  bool ReaderImpl::next(ColumnVectorBatch& data) {
    if (currentStripe > lastStripe) {
      data.numElements = 0;
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
    previousRow = firstRowOfStripe[currentStripe] + currentRowInStripe;
    currentRowInStripe += rowsToRead;
    if (currentRowInStripe >= rowsInCurrentStripe) {
      currentStripe += 1;
      currentRowInStripe = 0;
    }
    return rowsToRead != 0;
  }

  std::unique_ptr<ColumnVectorBatch> ReaderImpl::createRowBatch
       (const Type& type, uint64_t capacity) const {
    switch (static_cast<int>(type.getKind())) {
    case BOOLEAN:
    case BYTE:
    case SHORT:
    case INT:
    case LONG:
    case TIMESTAMP:
    case DATE: {
      LongVectorBatch* batch = new LongVectorBatch(capacity);
      return std::unique_ptr<ColumnVectorBatch>
        (dynamic_cast<ColumnVectorBatch*>(batch));
    }
    case FLOAT:
    case DOUBLE: {
      DoubleVectorBatch* batch = new DoubleVectorBatch(capacity);
      return std::unique_ptr<ColumnVectorBatch>
        (dynamic_cast<ColumnVectorBatch*>(batch));
    }
    case STRING:
    case BINARY:
    case CHAR:
    case VARCHAR: {
      StringVectorBatch* batch = new StringVectorBatch(capacity);
      return std::unique_ptr<ColumnVectorBatch>
        (dynamic_cast<ColumnVectorBatch*>(batch));
    }
    case STRUCT: {
      StructVectorBatch* structPtr = new StructVectorBatch(capacity);
      std::unique_ptr<ColumnVectorBatch> result
        (dynamic_cast<ColumnVectorBatch*>(structPtr));

      for(unsigned int i=0; i < type.getSubtypeCount(); ++i) {
        const Type& child = type.getSubtype(i);
        if (selectedColumns[static_cast<size_t>(child.getColumnId())]) {
          structPtr->fields.push_back(createRowBatch(child, capacity
                                                     ).release());
        }
      }
      return result;
    }
    case LIST:
    case MAP:
    case UNION:
    case DECIMAL:
    default:
      throw NotImplementedYet("not supported yet");
    }
  }

  std::unique_ptr<ColumnVectorBatch> ReaderImpl::createRowBatch
       (unsigned long capacity) const {
    return createRowBatch(*(schema.get()), capacity);
  }

  std::unique_ptr<Reader> createReader(std::unique_ptr<InputStream> stream, 
                                       const ReaderOptions& options) {
    return std::unique_ptr<Reader>(new ReaderImpl(std::move(stream), options));
  }


/**
 * start column statistics: min, max, length, numofvalues
 **/
// number of values
ColumnStatistics::ColumnStatistics(std::unique_ptr<ColumnStatisticsPrivate> data) :
    privateBits(std::unique_ptr<ColumnStatisticsPrivate> 
                (new ColumnStatisticsPrivate(data->columnStatistics))) 
{
}

ColumnStatistics::~ColumnStatistics() {

}

long ColumnStatistics::getNumberOfValues() const
{
    return privateBits->columnStatistics.numberofvalues();
}

// int
IntegerColumnStatistics::IntegerColumnStatistics(std::unique_ptr<ColumnStatisticsPrivate> data) : 
    ColumnStatistics(std::move(data))
{

}
long IntegerColumnStatistics::getMinimum() const
{
    if(getNumberOfValues()){
        return privateBits->columnStatistics.intstatistics().minimum();
    }
    throw std::logic_error("Cannot get minimum value: No value in column");
}
long IntegerColumnStatistics::getMaximum() const
{
    if(getNumberOfValues()){
        return privateBits->columnStatistics.intstatistics().maximum();
    }
    throw std::logic_error("Cannot get maximum value: No value in column");
}
bool IntegerColumnStatistics::isSumDefined() const
{
    if(privateBits->columnStatistics.intstatistics().has_sum() &&
       privateBits->columnStatistics.intstatistics().sum() <= std::numeric_limits<long>::max() ){
        return true;
    }
    return false;
}
long IntegerColumnStatistics::getSum() const
{
    if(IntegerColumnStatistics::isSumDefined()){
        return privateBits->columnStatistics.intstatistics().sum();
    }
    throw std::logic_error("sum is not defined");
}

// double
DoubleColumnStatistics::DoubleColumnStatistics(std::unique_ptr<ColumnStatisticsPrivate> data) : 
    ColumnStatistics(std::move(data))
{
}
double DoubleColumnStatistics::getMinimum() const
{
    if(getNumberOfValues()){
        return privateBits->columnStatistics.doublestatistics().minimum();
    }
    throw std::logic_error("Cannot get minimum value: No value in column");
}

double DoubleColumnStatistics::getMaximum() const
{
    if(getNumberOfValues()){
        return privateBits->columnStatistics.doublestatistics().maximum();
    }
    throw std::logic_error("Cannot get minimum value: No value in column");
}
double DoubleColumnStatistics::getSum() const
{
    if(privateBits->columnStatistics.doublestatistics().has_sum()){
        return privateBits->columnStatistics.doublestatistics().sum();
    }
    throw std::logic_error("sum is not defined");
}

// string
StringColumnStatistics::StringColumnStatistics(std::unique_ptr<ColumnStatisticsPrivate> data) : 
    ColumnStatistics(std::move(data))
{
}
std::string StringColumnStatistics::getMinimum() const
{
    return privateBits->columnStatistics.stringstatistics().minimum();
}

std::string StringColumnStatistics::getMaximum() const
{
    return privateBits->columnStatistics.stringstatistics().maximum();
}
long StringColumnStatistics::getTotalLength() const
{
    return privateBits->columnStatistics.stringstatistics().sum();
}

// boolean
BooleanColumnStatistics::BooleanColumnStatistics(std::unique_ptr<ColumnStatisticsPrivate> data) : 
    ColumnStatistics(std::move(data))
{
}
long BooleanColumnStatistics::getFalseCount() const
{
    return privateBits->columnStatistics.numberofvalues() - 
      privateBits->columnStatistics.bucketstatistics().count_size();
}

long BooleanColumnStatistics::getTrueCount() const
{
    return privateBits->columnStatistics.bucketstatistics().count_size();
}
// date
DateColumnStatistics::DateColumnStatistics(std::unique_ptr<ColumnStatisticsPrivate> data) : 
    ColumnStatistics(std::move(data))
{
}
long DateColumnStatistics::getMinimum() const
{
    return privateBits->columnStatistics.datestatistics().minimum();
}

long DateColumnStatistics::getMaximum() const
{
    return privateBits->columnStatistics.datestatistics().maximum();
}


// decimal
DecimalColumnStatistics::DecimalColumnStatistics(std::unique_ptr<ColumnStatisticsPrivate> data) : 
    ColumnStatistics(std::move(data))
{
}
Decimal DecimalColumnStatistics::getMinimum() const
{
    return stringToDecimal(privateBits->columnStatistics.decimalstatistics().minimum());
}

Decimal DecimalColumnStatistics::getMaximum() const
{
    return stringToDecimal(privateBits->columnStatistics.decimalstatistics().maximum());
}
Decimal DecimalColumnStatistics::getSum() const
{
    if(privateBits->columnStatistics.decimalstatistics().has_sum())
    {
        return stringToDecimal(privateBits->columnStatistics.decimalstatistics().sum());
    }
    throw std::logic_error("sum is not defined");
}

// timestamp
TimestampColumnStatistics::TimestampColumnStatistics(std::unique_ptr<ColumnStatisticsPrivate> data) : 
    ColumnStatistics(std::move(data))
{
}
long TimestampColumnStatistics::getMinimum() const
{
    return privateBits->columnStatistics.timestampstatistics().minimum();
}
long TimestampColumnStatistics::getMaximum() const
{
    return privateBits->columnStatistics.timestampstatistics().maximum();
}

// binary
BinaryColumnStatistics::BinaryColumnStatistics(std::unique_ptr<ColumnStatisticsPrivate> data) : 
    ColumnStatistics(std::move(data))
{
}
long BinaryColumnStatistics::getTotalLength() const
{
    return privateBits->columnStatistics.binarystatistics().sum();
}

}// namespace
