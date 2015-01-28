#include <iostream>
#include <fstream>
#include <string>
#include <sstream>
#include <iomanip>
#include "orc_proto.pb.h"


using namespace orc;

long getTotalPaddingSize(Footer footer);
StripeFooter readStripeFooter(StripeInformation stripe);

std::ifstream input;

int main(int argc, char* argv[])
{
    GOOGLE_PROTOBUF_VERIFY_VERSION;

    // Read the input arguments
    const char ROWINDEX_PREFIX[] = "--rowindex=";
    const int ROWINDEX_PREFIX_LENGTH = strlen(ROWINDEX_PREFIX);

    std::vector<char*> files;
    std::vector<int> rowIndexCols;

    char* pChar ;
    for (int i=1; i<argc; i++)
    {
        if (argv[i][0]=='-' & argv[i][1]=='-') {
            pChar = strstr(argv[i], ROWINDEX_PREFIX);
            if (pChar == argv[i]) { // the argument string starts with ROWINDEX_PREFIX
                pChar = strtok(argv[i]+ROWINDEX_PREFIX_LENGTH,",");
                while (pChar != NULL)
                {
                    rowIndexCols.push_back(atoi(pChar));
                    pChar = strtok(NULL,",");
                };
            } else {
                std::cout << "Unknown argument " << argv[i] << std::endl ;
                std::cout << "Usage: localfiledump <filename1> [... <filenameN>] [--rowindex=i1,i2,...,iN]" << std::endl ;
            };
        } else {
            files.push_back(argv[i]);
        };
    };

    for(int fileIx=0; fileIx<files.size(); fileIx++) {
        std::cout << "Structure for " << files[fileIx] << std::endl;

        // FIXIT: Instead of allocating memory for each part of the ORC file (postscript, footer, etc),
        // find a way to provide the stream + initial position + length to the ParseFromXXX() functions
        // Java implementation of protobuf has it, but C++ seems to lack.
        char* buffer ;

        // Determine the file size
        // FIXIT: Should be done using filesystem info
        input.open(files[fileIx], std::ios::in | std::ios::binary);
        input.seekg(0,input.end);
        unsigned int fileSize = input.tellg();

        // Read the postscript size
        input.seekg(fileSize-1);
        unsigned int postscriptSize = (int)input.get() ;

        // Read the postscript
        input.seekg(fileSize - postscriptSize-1);
        buffer = new char[postscriptSize];
        input.read(buffer, postscriptSize);
        PostScript postscript ;
        postscript.ParseFromArray(buffer, postscriptSize);
        delete[] buffer;
        std::cout << std::endl << "Postscript: " << std::endl ;
        postscript.PrintDebugString();

        // Everything but the postscript is compressed
        switch (static_cast<int>(postscript.compression())) {
        case NONE:
            //codec = NULL;
            break;
        case ZLIB:
            //codec = ZlibCodec(); break;
        case SNAPPY:
            //codec = SnappyCodec(); break;
        case LZO:
            //codec = LzoCodec(); break;
        default:
            std::cout << "Unsupported compression!" << std::endl ;
            input.close();
            return -1;
        };

        int footerSize = postscript.footerlength();
        int metadataSize = postscript.metadatalength();

        // Read the metadata
        input.seekg(fileSize - 1 - postscriptSize - footerSize - metadataSize);
        buffer = new char[metadataSize];
        input.read(buffer, metadataSize);
        Metadata metadata ;
        metadata.ParseFromArray(buffer, metadataSize);
        delete[] buffer;
        std::cout << std::endl << "Metadata: " << std::endl ;
        postscript.PrintDebugString();

        // Read the footer
        //input.seekg(fileSize -1 - postscriptSize-footerSize);
        buffer = new char[footerSize];
        input.read(buffer, footerSize);
        Footer footer ;
        footer.ParseFromArray(buffer, footerSize);
        delete[] buffer;
        std::cout << std::endl << "Footer: " << std::endl ;
        postscript.PrintDebugString();

        std::cout << std::endl << "Rows: " << footer.numberofrows() << std::endl;
        std::cout << "Compression: " << postscript.compression() << std::endl;
        if (postscript.compression() != NONE)
            std::cout << "Compression size: " << postscript.compressionblocksize() << std::endl;
        std::cout << "Type: " ;
        for (int typeIx=0; typeIx < footer.types_size(); typeIx++) {
            Type type = footer.types(typeIx);
            type.PrintDebugString();
        };

        std::cout << "\nStripe Statistics:" << std::endl;

        StripeInformation stripe ;
        Stream section;
        ColumnEncoding encoding;
        for (int stripeIx=0; stripeIx<footer.stripes_size(); stripeIx++)
        {
            std::cout << "  Stripe " << stripeIx+1 <<": " << std::endl ;
            stripe = footer.stripes(stripeIx);
            stripe.PrintDebugString();

            long offset = stripe.offset() + stripe.indexlength() + stripe.datalength();
            int tailLength = stripe.footerlength();

            // read the stripe footer
            input.seekg(offset);
            buffer = new char[tailLength];
            input.read(buffer, tailLength);

            StripeFooter stripeFooter;
            stripeFooter.ParseFromArray(buffer, tailLength);
            //stripeFooter.PrintDebugString();
            long stripeStart = stripe.offset();
            long sectionStart = stripeStart;
            for (int streamIx=0; streamIx<stripeFooter.streams_size(); streamIx++) {
                section = stripeFooter.streams(streamIx);
                std::cout << "    Stream: column " << section.column()  << " section "
                  << section.kind() << " start: " << sectionStart << " length " << section.length() << std::endl;
                sectionStart += section.length();
            };
            for (int columnIx=0; columnIx<stripeFooter.columns_size(); columnIx++) {
                encoding = stripeFooter.columns(columnIx);
                std::cout << "    Encoding column " << columnIx << ": " << encoding.kind() ;
                if (encoding.kind() == ColumnEncoding_Kind_DICTIONARY || encoding.kind() == ColumnEncoding_Kind_DICTIONARY_V2)
                    std::cout << "[" << encoding.dictionarysize() << "]";
                std::cout << std::endl;
            };

            /*
               if (!rowIndexCols.empty()) {
               std::vector<RowIndex> indices;
               long offset = stripeStart;

               stripeFooter = readStripeFooter(stripe);

               for(int streamIx=0; streamIx< stripeFooter.streams_size();streamIx++) {
               section = stripeFooter.streams(streamIx);
               if (section.kind() == Stream_Kind_ROW_INDEX) {
               int col = section.column();

               int streamLength = section.length();
               char* bufferRowIndex = new char[streamLength];
               input.seekg(offset);
               input.read(bufferRowIndex, streamLength);
               RowIndex rowIndex;
               rowIndex.ParseFromArray(bufferRowIndex, streamLength);
               delete[] bufferRowIndex ;
               indices.push_back(rowIndex);
               };
               offset += section.length();
               };

               for (int rowIx = 0; rowIx < rowIndexCols.size(); rowIx++) {
               int col = rowIndexCols[rowIx];
               std::cout << "    Row group index column " << col << ":" ;
               if (col >= indices.size())  {
               std::cout << " not found" << std::endl ;
               continue;
               };
            RowIndex index = indices[col];
            for (int entryIx = 0; entryIx < index.entry_size(); ++entryIx) {
                std::cout << "\n      Entry " << entryIx << ":";
                RowIndexEntry entry = index.entry(entryIx);
                ColumnStatistics colStats = entry.statistics();
                std::cout << " number of values: " << colStats.numberofvalues() ;
                for (int posIx = 0; posIx < entry.positions_size(); ++posIx) {
                    if (posIx != 0)
                        std::cout << ",";
                    std::cout << entry.positions(posIx);
                };
            };
            std::cout <<std::endl ;
    };
    };*/
        };

        long paddedBytes = getTotalPaddingSize(footer);
        // empty ORC file is ~45 bytes. Assumption here is file length always >0
        double percentPadding = ((double) paddedBytes / (double) fileSize) * 100;
        std::cout << "File length: " << fileSize << " bytes" << std::endl;
        std::cout <<"Padding length: " << paddedBytes << " bytes" << std::endl;
        std::cout <<"Padding ratio: " << std::fixed << std::setprecision(2) << percentPadding << " %" << std::endl;

        input.close();
    };

    google::protobuf::ShutdownProtobufLibrary();

    return 0;
}


StripeFooter readStripeFooter(StripeInformation stripe) {
    long offset = stripe.offset() + stripe.indexlength() + stripe.datalength();
    int tailLength = (int) stripe.footerlength();

    char* buffer = new char[tailLength];
    input.seekg(offset);
    input.read(buffer, tailLength);
    StripeFooter stripeFooter ;
    stripeFooter.ParseFromArray(buffer, tailLength);
    delete[] buffer;

    return stripeFooter;
}


long getTotalPaddingSize(Footer footer) {
    long paddedBytes = 0;
    StripeInformation stripe;
    for (int stripeIx=1; stripeIx<footer.stripes_size(); stripeIx++) {
        stripe = footer.stripes(stripeIx-1);
        long prevStripeOffset = stripe.offset();
        long prevStripeLen = stripe.datalength() + stripe.indexlength() + stripe.footerlength();
        paddedBytes += footer.stripes(stripeIx).offset() - (prevStripeOffset + prevStripeLen);
    };
    return paddedBytes;
}


