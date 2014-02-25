/* 
 * File:   common.hpp
 * Author: felix
 *
 * Created on May 29, 2013, 1:41 PM
 */

#ifndef COMMON_HPP
#define	COMMON_HPP

#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <assert.h>
#include <stdexcept>

#if defined (VAMPIR)
#include <vt_user.h>
#else
#define VT_TRACER(_arg)
#define VT_ON()
#define VT_OFF()
#define VT_USER_START()
#define VT_USER_END()
#endif

#define VERBOSE_NONE 0
#define VERBOSE_BASIC 1
#define VERBOSE_ALL 2
#define VERBOSE_ANNOY 3

//#define MPI_CP_MERGE

namespace cdm
{
    class RTException : virtual public std::runtime_error
    {
    public:

        RTException(const char *format, ...) :
        std::runtime_error("Runtime exception")
        {
            va_list args;
            printf("Runtime Error: ");
            va_start(args, format);
            vprintf(format, args);
            va_end(args);
            printf("\n");
            assert(0);
        }
    };
}

#endif	/* COMMON_HPP */

