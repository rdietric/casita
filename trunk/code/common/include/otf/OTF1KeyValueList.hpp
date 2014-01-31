/* 
 * File:   OTF1KeyValueList.hpp
 * Author: felix
 *
 * Created on May 7, 2013, 11:11 AM
 */

#ifndef OTF1KEYVALUELIST_HPP
#define	OTF1KEYVALUELIST_HPP

#include <otf.h>
#include "IKeyValueList.hpp"

namespace cdm
{
    namespace io
    {

        class OTF1KeyValueList : public IKeyValueList
        {
        public:
            
            OTF1KeyValueList() :
            list(NULL)
            {

            }

            OTF1KeyValueList(OTF_KeyValueList_struct *list) :
            list(list)
            {

            }
            
            void setList(OTF_KeyValueList_struct *list)
            {
                this->list = list;
            }

            uint8_t getUInt32(uint32_t key, uint32_t *value)
            {
                return OTF_KeyValueList_getUint32(list, key, value);
            }

            uint8_t getUInt64(uint32_t key, uint64_t *value)
            {
                return OTF_KeyValueList_getUint64(list, key, value);
            }

            uint8_t getInt32(uint32_t key, int32_t *value)
            {
                return OTF_KeyValueList_getInt32(list, key, value);
            }
            
        private:
            OTF_KeyValueList *list;
        };
    }
}

#endif	/* OTF1KEYVALUELIST_HPP */

