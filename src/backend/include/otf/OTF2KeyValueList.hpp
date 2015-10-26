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

#include <otf2/otf2.h>
#include "IKeyValueList.hpp"

namespace casita
{
 namespace io
 {

  class OTF2KeyValueList :
    public IKeyValueList
  {
    public:

      OTF2KeyValueList( ) :
        list( NULL )
      {

      }

      OTF2KeyValueList( OTF2_AttributeList_struct* list ) :
        list( list )
      {

      }

      void
      setList( OTF2_AttributeList_struct* list )
      {
        this->list = list;
      }

      KeyValueResult
      getUInt32( uint32_t key, uint32_t* value )
      {
        return ( OTF2_AttributeList_GetUint32( list, key,
                                               value ) == OTF2_SUCCESS ) ?
               KV_SUCCESS : KV_ERROR;
      }

      KeyValueResult
      getLocationRef( uint32_t key, uint64_t* value )
      {
        if( OTF2_AttributeList_TestAttributeByID(list, key) )
        {
          OTF2_ErrorCode otf2error = 
                OTF2_AttributeList_GetLocationRef( list, key, value );
          
          return ( otf2error == OTF2_SUCCESS ) ? KV_SUCCESS : KV_ERROR;
        }
        else
        {
          return KV_ERROR;
        }
      }

      KeyValueResult
      getUInt64( uint32_t key, uint64_t* value )
      {
        return ( OTF2_AttributeList_GetUint64( list, key,
                                               value ) == OTF2_SUCCESS ) ?
               KV_SUCCESS : KV_ERROR;
      }

      KeyValueResult
      getInt32( uint32_t key, int32_t* value )
      {
        return ( OTF2_AttributeList_GetInt32( list, key,
                                              value ) == OTF2_SUCCESS ) ?
               KV_SUCCESS : KV_ERROR;
      }

      uint32_t
      getSize( )
      {
        return OTF2_AttributeList_GetNumberOfElements( list );
      }

    private:
      OTF2_AttributeList* list;
  };
 }
}
