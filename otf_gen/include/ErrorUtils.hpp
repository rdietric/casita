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

        void setNoExceptions()
        {
            throwExceptions = false;
            fprintf(stderr, "Disabled exceptions\n");
        }

    private:
        bool throwExceptions;

        ErrorUtils() :
        throwExceptions(true)
        {

        }
        
        ErrorUtils(const ErrorUtils& utils)
        {
            
        }
    };

}

#endif	/* ERRORUTILS_HPP */

