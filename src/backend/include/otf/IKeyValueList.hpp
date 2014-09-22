/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2013-2014,
 * Technische Universitaet Dresden, Germany
 *
 * This software may be modified and distributed under the terms of
 * a BSD-style license. See the COPYING file in the package base
 * directory for details.
 *
 */

#pragma once

#include <stdint.h>

namespace casita
{
 namespace io
 {

  class IKeyValueList
  {
    public:
      enum KeyValueResult
      {
        KV_SUCCESS = 0,
        KV_ERROR   = 1
      };

      virtual KeyValueResult
      getUInt32( uint32_t key, uint32_t* value )      = 0;

      virtual KeyValueResult
      getUInt64( uint32_t key, uint64_t* value )      = 0;

      virtual KeyValueResult
      getInt32( uint32_t key, int32_t* value )        = 0;

      virtual KeyValueResult
      getLocationRef( uint32_t key, uint64_t* value ) = 0;

      virtual uint32_t
      getSize( ) = 0;

  };
 }
}
