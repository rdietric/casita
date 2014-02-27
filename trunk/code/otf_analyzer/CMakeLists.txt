# Projekt name

cmake_minimum_required(VERSION 2.8.5)

# enable VampirTrace
OPTION(VAMPIR "enable tracing using Score-P" OFF)
IF(VAMPIR)
	SET(CMAKE_CXX_COMPILER "scorep_cxx")
ENDIF(VAMPIR)

PROJECT(otfanalyzer)

# set helper pathes to find libraries and packages
SET(CMAKE_PREFIX_PATH "/usr/lib/x86_64-linux-gnu/" "$ENV{MPI_ROOT}" "$ENV{CUDA_ROOT}" "$ENV{BOOST_ROOT}")

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

# enable VampirTrace
IF(VAMPIR)
	ADD_DEFINITIONS(-DVAMPIR)
	message("building with Score-P tracing support")
ENDIF(VAMPIR)

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

# include OTF1
find_path(PROJ_OTF1_DIR
  NAMES include/open-trace-format/otf.h lib/libopen-trace-format.so
  PATHS ENV OTF_ROOT
  DOC "OTF1 ROOT location"
)

IF(PROJ_OTF1_DIR)
    MESSAGE(STATUS "Found OTF1: "${PROJ_OTF1_DIR})
    INCLUDE_DIRECTORIES(SYSTEM ${PROJ_OTF1_DIR}/include)
    LINK_DIRECTORIES(${PROJ_OTF1_DIR}/lib)
    set(LIBS ${LIBS} open-trace-format)
ELSE(PROJ_OTF1_DIR)
    MESSAGE(STATUS "Could NOT find OTF1")
ENDIF(PROJ_OTF1_DIR)


# include OTF2
find_path(PROJ_OTF2_DIR
  NAMES include/otf2/otf2.h lib/libotf2.so
  PATHS ENV OTF2_ROOT
  DOC "OTF2 ROOT location"
)

IF(PROJ_OTF2_DIR)
    MESSAGE(STATUS "Found OTF2: "${PROJ_OTF2_DIR})
    INCLUDE_DIRECTORIES(SYSTEM ${PROJ_OTF2_DIR}/include)
    LINK_DIRECTORIES(${PROJ_OTF2_DIR}/lib)
    set(LIBS ${LIBS} otf2)
ELSE(PROJ_OTF2_DIR)
    MESSAGE(STATUS "Could NOT find OTF2")
ENDIF(PROJ_OTF2_DIR)


# include Boost
FIND_PACKAGE(Boost REQUIRED COMPONENTS program_options regex)
INCLUDE_DIRECTORIES(SYSTEM ${Boost_INCLUDE_DIRS})
SET(LIBS ${LIBS} ${Boost_LIBRARIES})

# own includes
INCLUDE_DIRECTORIES("${CMAKE_SOURCE_DIR}/include" 
                    "${CMAKE_SOURCE_DIR}/../common/include")

file(GLOB SRCFILES "*.cpp")
file(GLOB COMMON_SRCFILES "${CMAKE_SOURCE_DIR}/../common/*.cpp")

add_executable(otfanalyzer ${COMMON_SRCFILES} ${SRCFILES})

target_link_libraries (otfanalyzer ${LIBS})

INSTALL(TARGETS otfanalyzer RUNTIME DESTINATION .)
