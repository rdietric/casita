/* 
 * File:   OTF2KeyValueList.hpp
 * Author: stolle
 *
 * Created on March 3, 2014, 1:23 PM
 */

#ifndef OTF2KEYVALUELIST_HPP
#define	OTF2KEYVALUELIST_HPP

#include <otf2/otf2.h>
#include "IKeyValueList.hpp"

namespace cdm
{
    namespace io
    {

        class OTF2KeyValueList : public IKeyValueList
        {
        public:
            
            OTF2KeyValueList() :
            list(NULL)
            {

            }

            OTF2KeyValueList(OTF2_AttributeList_struct *list) :
            list(list)
            {

            }
            
            void setList(OTF2_AttributeList_struct *list)
            {
                this->list = list;
            }

            uint8_t getUInt32(uint32_t key, uint32_t *value)
            {
                return OTF2_AttributeList_GetUint32(list, key, value);
            }

            uint8_t getLocationRef(uint32_t key, uint64_t *value)
            {
                return OTF2_AttributeList_GetLocationRef(list, key, value);
            }
            
            uint8_t getUInt64(uint32_t key, uint64_t *value)
            {
                return OTF2_AttributeList_GetUint64(list, key, value);
            }

            uint8_t getInt32(uint32_t key, int32_t *value)
            {
                return OTF2_AttributeList_GetInt32(list, key, value);
            }
            
            uint32_t getSize()
            {
                return OTF2_AttributeList_GetNumberOfElements(list);
            }
            
        private:
            OTF2_AttributeList *list;
        };
    }
}

#endif	/* OTF2KEYVALUELIST_HPP */

