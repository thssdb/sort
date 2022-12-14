# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

PROJECT(SessionExample)
CMAKE_MINIMUM_REQUIRED(VERSION 3.7)

SET(CMAKE_CXX_STANDARD 11)
SET(CMAKE_CXX_STANDARD_REQUIRED ON)
SET(CMAKE_POSITION_INDEPENDENT_CODE ON)

INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/../target/client/include)

FIND_PACKAGE(Boost REQUIRED)
IF (DEFINED BOOST_INCLUDEDIR)
    INCLUDE_DIRECTORIES(${Boost_INCLUDE_DIR})
ENDIF()

IF(MSVC)
    LINK_DIRECTORIES(${CMAKE_SOURCE_DIR}/../target/client/lib/Release)
ENDIF()
LINK_DIRECTORIES(${CMAKE_SOURCE_DIR}/../target/client/lib)

ADD_EXECUTABLE(SessionExample SessionExample.cpp)
ADD_EXECUTABLE(AlignedTimeseriesSessionExample AlignedTimeseriesSessionExample.cpp)

IF(MSVC)
    TARGET_LINK_LIBRARIES(SessionExample iotdb_session thriftmd)
    TARGET_LINK_LIBRARIES(AlignedTimeseriesSessionExample iotdb_session thriftmd)
ELSE()
    TARGET_LINK_LIBRARIES(SessionExample iotdb_session pthread)
    TARGET_LINK_LIBRARIES(AlignedTimeseriesSessionExample iotdb_session pthread)
ENDIF()
