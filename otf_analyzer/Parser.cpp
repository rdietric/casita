#include <iostream>
#include <sstream>
#include <fstream>
#include <vector>

#include "Parser.hpp"

#include <boost/program_options.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/cmdline.hpp>
#include <boost/program_options/variables_map.hpp>

namespace cdm
{
    namespace po = boost::program_options;

    Parser::Parser()
    {

    }

    Parser::Parser(Parser&)
    {

    }

    template <class T>
    bool from_string(T& t,
            const std::string& s,
            std::ios_base& (*f)(std::ios_base&))
    {
        std::istringstream iss(s);
        if ((iss >> f >> t).fail())
            throw std::invalid_argument("conversion invalid!");

        return true;
    }

    Parser& Parser::getInstance()
    {
        static Parser instance;
        return instance;
    }

    bool Parser::init(int argc, char** argv)
    throw (std::runtime_error)
    {
        options.createGraphs = false;
        options.createOTF = false;
        options.eventsProcessed = 0;
        options.filename = "";
        options.mergeActivities = false;
        options.noErrors = false;
        options.outOtfFile = "";
        options.printCriticalPath = false;
        options.verbose = 0;
        options.memLimit = 4 * 1024 * 1024; // 4GByte

        try
        {
            // add help message as options description
            std::stringstream desc_stream;
            desc_stream << "Usage: otfanalyzer <otf-file> [options]" << std::endl;
            po::options_description desc(desc_stream.str());

            // add standard options
            desc.add_options()
                    ("help,h", "print help message")

                    ("input", po::value<std::string > (&options.filename), "input OTF file")
                    ("output,o", po::value<std::string > (&options.outOtfFile), "output OTF file")
                    ("aggregate,a", po::value<bool> (&options.mergeActivities)->zero_tokens(), "aggregate statistics and print summary")
                    ("path,p", po::value<bool> (&options.printCriticalPath)->zero_tokens(), "print critical paths")
                    ("verbose,v", po::value<int>(&options.verbose), "verbosity level")
                    ("mem-limit", po::value<int>(&options.memLimit), "set memory limit (MB)")
                    ("no-errors", po::value<bool>(&options.noErrors), "ignore non-fatal analysis errors")
                    ;

            po::positional_options_description pos_options_descr;
            pos_options_descr.add("input", 1);

            // parse command line options and config file and store values in vm
            po::variables_map vm;
            po::store(po::command_line_parser(argc, argv).options(desc).positional(pos_options_descr).run(), vm);

            po::notify(vm);

            // print help message and quit simulation
            if (vm.count("help"))
            {
                std::cout << desc << "\n";
                return false;
            }
            
            if (vm.count("output"))
            {
                options.createOTF = true;
            }

            if (vm.count("input") != 1)
            {
                std::cout << "Please specify exactly one input OTF file." << std::endl;
                std::cout << desc << "\n";
                return false;
            }

        } catch (boost::program_options::error& e)
        {
            std::cerr << e.what() << std::endl;
            return false;
        }

        return true;
    }

    ProgramOptions& Parser::getProgramOptions()
    {
        return options;
    }

}