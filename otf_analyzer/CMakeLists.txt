# Projekt name

cmake_minimum_required(VERSION 2.6)

# enable VampirTrace
OPTION(VAMPIR "enable tracing" OFF)
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
    #INCLUDE_DIRECTORIES($ENV{VT_INC})
    #ADD_DEFINITIONS(-DVTRACE)
    #SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
	message("building with tracing support")
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

# include OTF
INCLUDE_DIRECTORIES($ENV{OTF_INC} $ENV{OTF2_INC} $ENV{VT_INC}/vampirtrace)
LINK_DIRECTORIES($ENV{OTF_LIB} $ENV{OTF2_LIB} $ENV{VT_LIB})
SET(LIBS ${LIBS} open-trace-format otf2)

# include Boost
FIND_PACKAGE(Boost REQUIRED COMPONENTS program_options regex)
INCLUDE_DIRECTORIES(SYSTEM ${Boost_INCLUDE_DIRS})
SET(LIBS ${LIBS} ${Boost_LIBRARIES})

# own includes
INCLUDE_DIRECTORIES("${CMAKE_SOURCE_DIR}/include" "${CMAKE_SOURCE_DIR}/include/cuda" 
                    "${CMAKE_SOURCE_DIR}/../common/include")

file(GLOB SRCFILES "*.cpp")
file(GLOB COMMON_SRCFILES "${CMAKE_SOURCE_DIR}/../common/*.cpp")

add_executable(otfanalyzer ${COMMON_SRCFILES} ${SRCFILES})

target_link_libraries (otfanalyzer ${LIBS})

INSTALL(TARGETS otfanalyzer RUNTIME DESTINATION .)
