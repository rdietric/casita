cmake_minimum_required(VERSION 2.8.5)

PROJECT(casita)

SET(PROJECT_VERSION 1.9)
ADD_DEFINITIONS(-DCASITA_VERSION="${PROJECT_VERSION}")

# SET helper paths to find libraries and packages
#SET(CMAKE_PREFIX_PATH "/usr/lib/x86_64-linux-gnu/" "$ENV{MPI_ROOT}" "$ENV{MPICH_DIR}" "$ENV{SCOREP_ROOT}")
SET(CMAKE_PREFIX_PATH "$ENV{SCOREP_ROOT}")

# install prefix
IF(CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)
    SET(CMAKE_INSTALL_PREFIX "${PROJECT_BINARY_DIR}" CACHE PATH "install prefix" FORCE)
ENDIF(CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)

# set '-g' flag for symbol information
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g")

# use -Wall for gcc
IF (CMAKE_COMPILER_IS_GNUCXX OR CMAKE_COMPILER_IS_GNUC)
  IF(NOT CMAKE_CXX_FLAGS MATCHES "-Wall$")
    SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wno-pmf-conversions -Wno-deprecated")
  ENDIF(NOT CMAKE_CXX_FLAGS MATCHES "-Wall$")

  IF(NOT CMAKE_C_FLAGS MATCHES "-Wall$")
    SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wno-pmf-conversions -Wno-deprecated")
  ENDIF(NOT CMAKE_C_FLAGS MATCHES "-Wall$")

  MESSAGE(STATUS "Compiler warnings are activated")
ELSE ()
  MESSAGE(STATUS "Compiler warnings are deactivated")
ENDIF ()

SET(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR};${CMAKE_MODULE_PATH}")

################################# DEBUGGING ####################################
# enable release build
OPTION(RELEASE "Disable all debug asserts" OFF)
IF(NOT RELEASE)
    SET(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g")
    SET(CMAKE_BUILD_TYPE Debug)
    ADD_DEFINITIONS(-DDEBUG=0)
    message("building debug")
ELSE()
    message("building release")
    SET(CMAKE_BUILD_TYPE Release)
	  SET(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3")
ENDIF(NOT RELEASE)

# enable debugging categories
SET(DEBUG_LEVEL CACHE STRING "Debugging Category (0=no, 1=nbMPI, 2=CPA)")
#OPTION(DEBUG "Disable all debug asserts" OFF)
IF(DEBUG_LEVEL GREATER 0  AND DEBUG_LEVEL LESS 3 )
    #check variable such that it makes sense
    IF( DEBUG_LEVEL EQUAL 1 )
      MESSAGE( "... for non-blocking MPI analysis" )
    ENDIF( )
    
    IF( DEBUG_LEVEL EQUAL 2 )
      MESSAGE( "... for critical-path analysis" )
    ENDIF( )

    SET(FLAGS_TO_USE CMAKE_CXX_FLAGS)
    IF(CMAKE_BUILD_TYPE)
      STRING(TOUPPER ${CMAKE_BUILD_TYPE} TEMP)
      SET(FLAGS_TO_USE "${FLAGS_TO_USE}_${TEMP}")
      #MESSAGE("${FLAGS_TO_USE} ${TEMP}")
    ENDIF()
    SET(${FLAGS_TO_USE} "${${FLAGS_TO_USE}} -DDEBUG_LEVEL=${DEBUG_LEVEL}")
    message("... with debug flags ${${FLAGS_TO_USE}}")
ENDIF()
################################################################################

# enable gprof
OPTION(GPROF "enable gprof" OFF)
IF(GPROF)
  SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pg")
  message("building with gprof")
ENDIF(GPROF)

# include MPI
FIND_PACKAGE(MPI REQUIRED)
#IF(MPI_FOUND)
  INCLUDE_DIRECTORIES(SYSTEM ${MPI_C_INCLUDE_PATH} ${MPI_CXX_INCLUDE_PATH})
  SET(LIBS ${LIBS} ${MPI_C_LIBRARIES} ${MPI_CXX_LIBRARIES})
#ELSE()
#  message( FATAL_ERROR "Could not find MPI!")
#ENDIF(MPI_FOUND)

#OTF2 support
FIND_PACKAGE(OTF2 REQUIRED)
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OTF2_CFLAGS}")
SET(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${OTF2_LDFLAGS}")
SET(LIBS ${LIBS} ${OTF2_LIBS})
FILE(GLOB SRCFILES_BACKEND_OTF2 "src/backend/otf/OTF2*.cpp")
ADD_DEFINITIONS(-DENABLE_OTF2=1)

#OpenMP support
OPTION(OMP "enable OpenMP" OFF)
IF(OMP)
  FIND_PACKAGE(OpenMP)
  IF(OPENMP_FOUND)
  SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS} ${OpenMP_C_FLAGS}")
  ENDIF(OPENMP_FOUND)
  message("Use OpenMP parallel execution!")
ENDIF(OMP)

# include Boost
#FIND_PACKAGE(Boost COMPONENTS program_options regex filesystem system)
#IF(Boost_FOUND)
#  INCLUDE_DIRECTORIES(SYSTEM ${Boost_INCLUDE_DIRS})
#  SET(LIBS ${LIBS} ${Boost_LIBRARIES})
#  ADD_DEFINITIONS(-DBOOST_AVAILABLE)
#ENDIF(Boost_FOUND)

# own includes
INCLUDE_DIRECTORIES("${CMAKE_SOURCE_DIR}/src/backend/include" 
                    "${CMAKE_SOURCE_DIR}/src/frontend/include"
                    "${CMAKE_SOURCE_DIR}/src/analysis/include")

FILE(GLOB SRCFILES_BACKEND "src/backend/*.cpp")
FILE(GLOB SRCFILES_FRONTEND "src/frontend/*.cpp")
FILE(GLOB SRCFILES_ANALYSIS "src/analysis/*.cpp")

OPTION(BUILD_STATIC "static build" OFF)
IF(BUILD_STATIC)
  SET(CMAKE_FIND_LIBRARY_SUFFIXES ".a")
  SET(BUILD_SHARED_LIBRARIES OFF)
  SET(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -static")
  message("building executable with static libraries")
ENDIF(BUILD_STATIC)

ADD_EXECUTABLE(casita ${SRCFILES_BACKEND}
                      ${SRCFILES_BACKEND_OTF2}
                      ${SRCFILES_FRONTEND}
                      ${SRCFILES_ANALYSIS})

TARGET_LINK_LIBRARIES(casita ${LIBS})

INSTALL(TARGETS casita RUNTIME DESTINATION bin)

# Make distribution package
SET(CPACK_GENERATOR "TGZ")
SET(CPACK_SOURCE_GENERATOR "TGZ")
SET(CPACK_PACKAGE_DESCRIPTION_SUMMARY "Critical path AnalySIs Tool for heterogeneous Applications")
SET(CPACK_PACKAGE_VENDOR "TU Dresden, ZIH with GWT-TUD GmbH")
SET(CPACK_RESOURCE_FILE_LICENSE "${CMAKE_CURRENT_SOURCE_DIR}/COPYING")
set(CPACK_SOURCE_PACKAGE_FILE_NAME "${CMAKE_PROJECT_NAME}-${PROJECT_VERSION}")
set(CPACK_SOURCE_IGNORE_FILES "/BUILD/;/tests/;/.git/;.gitignore;/.svn/;svn-commit*;~$;casita$;casita_prj;nbproject;/CMakeFiles/;cmake_install.cmake;CMakeCache.txt;Makefile;CPack*;compile*;")
INCLUDE(CPack)

add_custom_target(dist COMMAND ${CMAKE_MAKE_PROGRAM} package_source)
