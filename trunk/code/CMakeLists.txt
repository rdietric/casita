cmake_minimum_required(VERSION 2.8.5)

PROJECT(casita)

# SET helper pathes to find libraries and packages
SET(CMAKE_PREFIX_PATH "/usr/lib/x86_64-linux-gnu/" "$ENV{MPI_ROOT}" "$ENV{CUDA_ROOT}" "$ENV{BOOST_ROOT}" "$ENV{OTF_ROOT}" "$ENV{OTF2_ROOT}")

# install prefix
IF(CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)
    SET(CMAKE_INSTALL_PREFIX "${PROJECT_BINARY_DIR}" CACHE PATH "install prefix" FORCE)
ENDIF(CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT) 
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fopenmp -g -Wall -Wno-pmf-conversions -Wno-deprecated")

# enable release build
OPTION(RELEASE "disable all debug asserts" OFF)
IF(NOT RELEASE)
    SET(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g")
    SET(CMAKE_BUILD_TYPE Debug)
    ADD_DEFINITIONS(-DDEBUG)
    message("building debug")
ELSE()
    message("building release")
	SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2 -Werror")
ENDIF(NOT RELEASE)

# enable gprof
OPTION(GPROF "enable gprof" OFF)
IF(GPROF)
	SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pg")
	message("building with gprof")
ENDIF(GPROF)

# include MPI
FIND_PACKAGE(MPI REQUIRED)
INCLUDE_DIRECTORIES(SYSTEM ${MPI_C_INCLUDE_PATH} ${MPI_CXX_INCLUDE_PATH})
SET(LIBS ${LIBS} ${MPI_C_LIBRARIES} ${MPI_CXX_LIBRARIES})

# include CUDA
FIND_PACKAGE(CUDA REQUIRED)
INCLUDE_DIRECTORIES(${CUDA_INCLUDE_DIRS})

# include OTF1 support if found
FIND_PATH(PROJ_OTF1_DIR
  NAMES include/open-trace-format/otf.h lib/libopen-trace-format.so
  PATHS ENV OTF_ROOT
  DOC "OTF1 ROOT location"
)

IF(PROJ_OTF1_DIR)
    MESSAGE(STATUS "Found OTF1: "${PROJ_OTF1_DIR})
    INCLUDE_DIRECTORIES(SYSTEM ${PROJ_OTF1_DIR}/include)
    LINK_DIRECTORIES(${PROJ_OTF1_DIR}/lib)
    SET(LIBS ${LIBS} open-trace-format)
    ADD_DEFINITIONS(-DENABLE_OTF1=1)
    SET(OTF_FOUND YES)
    FILE(GLOB SRCFILES_BACKEND_OTF1 "src/backend/otf/OTF1*.cpp")
ELSE(PROJ_OTF1_DIR)
    MESSAGE(STATUS "Could NOT find OTF1")
ENDIF(PROJ_OTF1_DIR)


# include OTF2 support if found
FIND_PATH(PROJ_OTF2_DIR
  NAMES include/otf2/otf2.h lib/libotf2.so
  PATHS ENV OTF2_ROOT
  DOC "OTF2 ROOT location"
)

IF(PROJ_OTF2_DIR)
    MESSAGE(STATUS "Found OTF2: "${PROJ_OTF2_DIR})
    INCLUDE_DIRECTORIES(SYSTEM ${PROJ_OTF2_DIR}/include)
    LINK_DIRECTORIES(${PROJ_OTF2_DIR}/lib)
    SET(LIBS ${LIBS} otf2)
    ADD_DEFINITIONS(-DENABLE_OTF2=1)
    SET(OTF_FOUND YES)
    FILE(GLOB SRCFILES_BACKEND_OTF2 "src/backend/otf/OTF2*.cpp")
ELSE(PROJ_OTF2_DIR)
    MESSAGE(STATUS "Could NOT find OTF2")
ENDIF(PROJ_OTF2_DIR)


IF(NOT OTF_FOUND)
    MESSAGE(FATAL_ERROR "No supported OTF version found, cannot continue!")
ENDIF(NOT OTF_FOUND)


# include Boost
FIND_PACKAGE(Boost REQUIRED COMPONENTS program_options regex filesystem system)
INCLUDE_DIRECTORIES(SYSTEM ${Boost_INCLUDE_DIRS})
SET(LIBS ${LIBS} ${Boost_LIBRARIES})

# own includes
INCLUDE_DIRECTORIES("${CMAKE_SOURCE_DIR}/src/backend/include" 
                    "${CMAKE_SOURCE_DIR}/src/frontend/include"
                    "${CMAKE_SOURCE_DIR}/src/analysis/include")

FILE(GLOB SRCFILES_BACKEND "src/backend/*.cpp")
FILE(GLOB SRCFILES_FRONTEND "src/frontend/*.cpp")
FILE(GLOB SRCFILES_ANALYSIS "src/analysis/*.cpp")

ADD_EXECUTABLE(casita ${SRCFILES_BACKEND} ${SRCFILES_BACKEND_OTF1}
                      ${SRCFILES_BACKEND_OTF2} ${SRCFILES_FRONTEND}
                      ${SRCFILES_ANALYSIS})

TARGET_LINK_LIBRARIES(casita ${LIBS})

INSTALL(TARGETS casita RUNTIME DESTINATION .)
