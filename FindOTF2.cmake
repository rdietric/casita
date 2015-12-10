# CMake file
# Copyright (C) ZIH, Technische Universitaet Dresden, Federal Republic of Germany, 2011-2013
#
# See the file LICENSE.txt in the package base directory for details
#
# @file FindOTF2.cmake
#       Detects the OTF2 installation.
#
# @author Joachim Protze
# @date 11.11.2011


#variable_watch(OTF2_FOUND)
set(OTF2_FOUND FALSE)

# Check OTF2 version
#variable_watch(OTF2_VERSION)
execute_process(COMMAND otf2-config "--version" OUTPUT_VARIABLE OTF2_VERSION OUTPUT_STRIP_TRAILING_WHITESPACE)

# Check OTF2 compiler flags
#variable_watch(OTF2_CFLAGS)
execute_process(COMMAND otf2-config "--cflags" OUTPUT_VARIABLE OTF2_CFLAGS OUTPUT_STRIP_TRAILING_WHITESPACE)

# Check OTF2 libs
#variable_watch(OTF2_LIBS)
execute_process(COMMAND otf2-config "--libs" OUTPUT_VARIABLE OTF2_LIBS_RESULT OUTPUT_STRIP_TRAILING_WHITESPACE)
if(NOT "${OTF2_LIBS_RESULT}" STREQUAL "")
    string(REGEX REPLACE " +" ";" OTF2_LIBS_RESULT ${OTF2_LIBS_RESULT})

    # split -l flags from the remaining linker flags
    foreach(FLAG ${OTF2_LIBS_RESULT})
        string(REGEX MATCH "^-l[^ ]+" LIB ${FLAG})
        if(LIB STREQUAL "")
            list(APPEND OTF2_LDFLAGS ${FLAG})
        else(LIB STREQUAL "")
            string(REGEX REPLACE "^-l([^ ]+)" "\\1" LIB ${LIB})
            list(APPEND OTF2_LIBS ${LIB})
        endif(LIB STREQUAL "")
    endforeach(FLAG)
    unset(OTF2_LIBS_RESULT)
endif(NOT "${OTF2_LIBS_RESULT}" STREQUAL "")

# Check OTF2 linker flags
#variable_watch(OTF2_LIBS)
execute_process(COMMAND otf2-config "--ldflags" OUTPUT_VARIABLE OTF2_LIBS_RESULT OUTPUT_STRIP_TRAILING_WHITESPACE)
if(NOT "${OTF2_LIBS_RESULT}" STREQUAL "")
    string(REGEX REPLACE " +" ";" OTF2_LIBS_RESULT ${OTF2_LIBS_RESULT})

    # split -l flags from the remaining linker flags
    foreach(FLAG ${OTF2_LIBS_RESULT})
        string(REGEX MATCH "^-l[^ ]+" LIB ${FLAG})
        if(LIB STREQUAL "")
            list(APPEND OTF2_LDFLAGS ${FLAG})
        else(LIB STREQUAL "")
            string(REGEX REPLACE "^-l([^ ]+)" "\\1" LIB ${LIB})
            list(APPEND OTF2_LIBS ${LIB})
        endif(LIB STREQUAL "")
    endforeach(FLAG)
    unset(OTF2_LIBS_RESULT)
endif(NOT "${OTF2_LIBS_RESULT}" STREQUAL "")

if("${OTF2_CFLAGS}${OTF2_LDFLAGS}${OTF2_LIBS}" STREQUAL "")

    # OTF2 installation was not found
    if(OTF2_FIND_REQUIRED)
        message(FATAL_ERROR "OTF2 installation was not found. Make sure otf2-config is in $PATH.")
    else(OTF2_FIND_REQUIRED)
        message("OTF2 installation was not found. Make sure otf2-config is in $PATH.")
    endif(OTF2_FIND_REQUIRED)

else("${OTF2_CFLAGS}${OTF2_LDFLAGS}${OTF2_LIBS}" STREQUAL "")

    # OTF2 installation was found
    list(REVERSE OTF2_LIBS)
    list(REMOVE_DUPLICATES OTF2_LIBS)
    list(REVERSE OTF2_LIBS)
    string(REPLACE ";" " " OTF2_LDFLAGS "${OTF2_LDFLAGS}")

    message(STATUS "OTF2 found:")
    message(STATUS "   VERSION  = ${OTF2_VERSION}")
    message(STATUS "   CFLAGS   = ${OTF2_CFLAGS}")
    message(STATUS "   LDFLAGS  = ${OTF2_LDFLAGS}")
    message(STATUS "   LIBS     = ${OTF2_LIBS}")
    set(OTF2_FOUND TRUE)

endif("${OTF2_CFLAGS}${OTF2_LDFLAGS}${OTF2_LIBS}" STREQUAL "")
