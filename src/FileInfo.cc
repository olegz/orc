#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <sstream>
#include <iomanip>
//#include "orc_proto.pb.h"
#include "wrap/orc-proto-wrapper.hh"


using namespace orc::proto;

long getTotalPaddingSize(Footer footer);
StripeFooter readStripeFooter(StripeInformation stripe);

std::ifstream input;

int main(int argc, char* argv[])
{
    GOOGLE_PROTOBUF_VERIFY_VERSION;

    if (argc < 2) {
      std::cout << "Usage: file-metadata <filename>\n";
    }

    std::cout << "Structure for " << argv[1] << std::endl;

    input.open(argv[1], std::ios::in | std::ios::binary);
    input.seekg(0,input.end);
    unsigned int fileSize = input.tellg();

    // Read the postscript size
    input.seekg(fileSize-1);
    unsigned int postscriptSize = (int)input.get() ;

    // Read the postscript
    input.seekg(fileSize - postscriptSize-1);
    std::vector<char> buffer(postscriptSize) ;
    input.read(buffer.data(), postscriptSize);
    PostScript postscript ;
    postscript.ParseFromArray(buffer.data(), postscriptSize);
    std::cout << std::endl << "Postscript: " << std::endl ;
    postscript.PrintDebugString();

    // Everything but the postscript is compressed
    switch (static_cast<int>(postscript.compression())) {
    case NONE:
        break;
    case ZLIB:
    case SNAPPY:
    case LZO:
    default:
        std::cout << "ORC files with compression are not supported" << std::endl ;
        input.close();
        return -1;
    };

    int footerSize = postscript.footerlength();
    int metadataSize = postscript.metadatalength();

    // Read the metadata
    input.seekg(fileSize - 1 - postscriptSize - footerSize - metadataSize);
    buffer.resize(metadataSize);
    input.read(buffer.data(), metadataSize);
    Metadata metadata ;
    metadata.ParseFromArray(buffer.data(), metadataSize);
    std::cout << std::endl << "Metadata: " << std::endl ;

    // get stripe statistics from metadata
    std::cout << "Has " << metadata.stripestats_size() << " stripes in this file\n";
    for(int i = 0; i < metadata.stripestats_size(); ++i){
        // TODO for stripe
    }
    postscript.PrintDebugString();

    // Read the footer
    //input.seekg(fileSize -1 - postscriptSize-footerSize);
    buffer.resize(footerSize);
    input.read(buffer.data(), footerSize);
    Footer footer ;
    footer.ParseFromArray(buffer.data(), footerSize);
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

    std::cout << "Has " << footer.statistics_size() << " Columns statistics in file" << std::endl;
    for(int i = 0; i < footer.statistics_size(); ++i){
        ColumnStatistics col = footer.statistics(i);
        if(col.has_intstatistics()) {
          std::cout << "column " << i << "is INT.\n" 
                    << "Minimum = " << col.intstatistics().minimum() << std::endl
                    << "Maximum = " << col.intstatistics().maximum() << std::endl;
        }else if(col.has_stringstatistics()){
            std::cout << "column " << i << "is STRING.\n" 
                    << "Minimum = " << col.stringstatistics().minimum() << std::endl
                    << "Maximum = " << col.stringstatistics().maximum() << std::endl;
        }
        if(col.has_numberofvalues()){
            std::cout << "column has "<< col.numberofvalues() << " number of values\n";
        }
    }
    
    std::cout << "\nStripe Information:" << std::endl;

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
        buffer.resize(tailLength);
        input.read(buffer.data(), tailLength);

        StripeFooter stripeFooter;
        stripeFooter.ParseFromArray(buffer.data(), tailLength);
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
    };

    long paddedBytes = getTotalPaddingSize(footer);
    // empty ORC file is ~45 bytes. Assumption here is file length always >0
    double percentPadding = ((double) paddedBytes / (double) fileSize) * 100;
    std::cout << "File length: " << fileSize << " bytes" << std::endl;
    std::cout <<"Padding length: " << paddedBytes << " bytes" << std::endl;
    std::cout <<"Padding ratio: " << std::fixed << std::setprecision(2) << percentPadding << " %" << std::endl;

    input.close();



    google::protobuf::ShutdownProtobufLibrary();

    return 0;
}


StripeFooter readStripeFooter(StripeInformation stripe) {
    long offset = stripe.offset() + stripe.indexlength() + stripe.datalength();
    int tailLength = (int) stripe.footerlength();

    std::vector<char> buffer(tailLength);
    input.seekg(offset);
    input.read(buffer.data(), tailLength);
    StripeFooter stripeFooter ;
    stripeFooter.ParseFromArray(buffer.data(), tailLength);

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


