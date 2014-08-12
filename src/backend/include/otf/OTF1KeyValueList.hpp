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

#include <open-trace-format/otf.h>
#include "IKeyValueList.hpp"

namespace casita
{
 namespace io
 {

  class OTF1KeyValueList :
    public IKeyValueList
  {
    public:

      OTF1KeyValueList( ) :
        list( NULL )
      {

      }

      OTF1KeyValueList( OTF_KeyValueList_struct* list ) :
        list( list )
      {

      }

      void
      setList( OTF_KeyValueList_struct* list )
      {
        this->list = list;
      }

      uint8_t
      getLocationRef( uint32_t key, uint64_t* value )
      {
        uint32_t processId;
        uint8_t status = OTF_KeyValueList_getUint32( list, key, &processId );
        *value = (uint64_t)processId;
        return status;
      }

      uint8_t
      getUInt32( uint32_t key, uint32_t* value )
      {
        return OTF_KeyValueList_getUint32( list, key, value );
      }

      uint8_t
      getUInt64( uint32_t key, uint64_t* value )
      {
        return OTF_KeyValueList_getUint64( list, key, value );
      }

      uint8_t
      getInt32( uint32_t key, int32_t* value )
      {
        return OTF_KeyValueList_getInt32( list, key, value );
      }

      uint32_t
      getSize( )
      {
        return OTF_KeyValueList_getCount( list );
      }

    private:
      OTF_KeyValueList* list;
  };
 }
}
