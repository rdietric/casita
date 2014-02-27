#ifndef PARSER_HPP
#define	PARSER_HPP

#include <boost/program_options/options_description.hpp>

#include <string>
#include <stdexcept>
#include <vector>
#include <stdint.h>
#include <list>

namespace cdm
{
    namespace po = boost::program_options;

    typedef struct
    {
        bool createOTF;
        bool createGraphs;
        bool printCriticalPath;
        bool mergeActivities;
        bool noErrors;
        int verbose;
        int eventsProcessed;
        int memLimit;
        std::string outOtfFile;
        std::string filename;
    } ProgramOptions;

    class Parser
    {
    public:
        static Parser& getInstance();

        bool init(int argc, char **argv) throw (std::runtime_error);

        ProgramOptions& getProgramOptions();

    private:
        Parser();

        Parser(Parser& cc);

        ProgramOptions options;
    };

}

#endif	/* PARSER_HPP */

