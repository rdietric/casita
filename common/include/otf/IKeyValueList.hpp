/* 
 * File:   IKeyValueList.hpp
 * Author: felix
 *
 * Created on May 7, 2013, 11:05 AM
 */

#ifndef IKEYVALUELIST_HPP
#define	IKEYVALUELIST_HPP

#include <stdint.h>

namespace cdm
{
    namespace io
    {

        class IKeyValueList
        {
        public:
            virtual uint8_t getUInt32(uint32_t key, uint32_t *value) = 0;
            virtual uint8_t getUInt64(uint32_t key, uint64_t *value) = 0;
            virtual uint8_t getInt32(uint32_t key, int32_t *value) = 0;
            virtual uint8_t getLocationRef(uint32_t key, uint64_t *value) = 0;
            virtual uint32_t getSize() = 0;
        };
    }
}

#endif	/* IKEYVALUELIST_HPP */

