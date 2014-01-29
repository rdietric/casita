/* 
 * File:   ErrorUtils.hpp
 * Author: felix
 *
 * Created on 16. Oktober 2013, 16:37
 */

#ifndef ERRORUTILS_HPP
#define	ERRORUTILS_HPP

#include <stdio.h>
#include <cstdlib>
#include <cstdarg>
#include "common.hpp"

namespace cdm
{

    class ErrorUtils
    {
    public:

        static ErrorUtils &getInstance()
        {
            static ErrorUtils instance;
            return instance;
        }

        void throwError(const char *fmt, ...)
        {
            va_list argp;
            const size_t MAX_LEN = 512;
            char msg[MAX_LEN];

            va_start(argp, fmt);
            vsnprintf(msg, MAX_LEN, fmt, argp);
            va_end(argp);

            if (throwExceptions)
                throw RTException(msg);
            else
                fprintf(stderr, "%s\n", msg);
        }

        void throwFatalError(const char *fmt, ...)
        {
            va_list argp;
            const size_t MAX_LEN = 512;
            char msg[MAX_LEN];

            va_start(argp, fmt);
            vsnprintf(msg, MAX_LEN, fmt, argp);
            va_end(argp);

            throw RTException(msg);
        }

        void outputMessage(const char *fmt, ...)
        {
            if (verbose)
            {
                va_list argp;
                const size_t MAX_LEN = 512;
                char msg[MAX_LEN];

                va_start(argp, fmt);
                vsnprintf(msg, MAX_LEN, fmt, argp);
                va_end(argp);

                fprintf(stderr, "%s\n", msg);
            }
        }

        void setNoExceptions()
        {
            throwExceptions = false;
            fprintf(stderr, "Disabled exceptions\n");
        }

        void setVerbose()
        {
            verbose = true;
            fprintf(stderr, "Enabled verbose output for error utils\n");
        }

    private:
        bool throwExceptions;
        bool verbose;

        ErrorUtils() :
        throwExceptions(true),
        verbose(false)
        {

        }

        ErrorUtils(const ErrorUtils& utils)
        {

        }
    };

}

#endif	/* ERRORUTILS_HPP */

