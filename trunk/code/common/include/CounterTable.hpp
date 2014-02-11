/* 
 * File:   CounterTable.hpp
 * Author: felix
 *
 * Created on 27. Juni 2013, 12:59
 */

#ifndef COUNTERTABLE_HPP
#define	COUNTERTABLE_HPP

#include <stdint.h>
#include <map>
#include <set>
#include <otf.h>

namespace cdm
{
    enum CounterType
    {
        CTR_USER = 0,
        CTR_BLAME,               /* local blame for visualization */
        CTR_BLAME_STATISTICS,    /* accumulated blame for statistics */
        CTR_WAITSTATE,
        CTR_WAITSTATE_LOG10,
        CTR_CRITICALPATH,
    };

    typedef struct
    {
        CounterType type;
        const char *name;
        bool hasDefault;
        bool isInternal;
        uint64_t defaultValue;
        uint32_t otfMode;
    } CtrTableEntry;

    static const CtrTableEntry COUNTER_TABLE[] = {
        {CTR_USER, "Unknown Counter", false, false, 0,
                OTF_COUNTER_SCOPE_POINT | OTF_COUNTER_TYPE_ABS},
        {CTR_BLAME, "Exclusive Blame", true, false, 0,
                OTF_COUNTER_SCOPE_NEXT | OTF_COUNTER_TYPE_ABS},
        {CTR_BLAME_STATISTICS, "Blame Statistics", true, true, 0,
                OTF_COUNTER_SCOPE_NEXT | OTF_COUNTER_TYPE_ABS},
        {CTR_WAITSTATE, "Waiting Time", true, false, 0,
                OTF_COUNTER_SCOPE_NEXT | OTF_COUNTER_TYPE_ABS},
        {CTR_WAITSTATE, "Waiting Time (log10)", true, false, 0,
                OTF_COUNTER_SCOPE_NEXT | OTF_COUNTER_TYPE_ABS},
        {CTR_CRITICALPATH, "onCriticalPath", true, false, 0,
                OTF_COUNTER_SCOPE_NEXT | OTF_COUNTER_TYPE_ABS}
    };

    class CounterTable
    {
    private:
        typedef std::map<uint32_t, CtrTableEntry*> CtrEntryMap;

    public:
        
        typedef std::set<uint32_t> CtrIdSet;

        CounterTable() :
        maxCtrId(0)
        {

        }

        virtual ~CounterTable()
        {
            for (CtrEntryMap::const_iterator iter = counters.begin();
                    iter != counters.end(); ++iter)
            {
                delete iter->second;
            }
        }
        
        void addDefaultCounter(uint32_t ctrId, CounterType ctrType)
        {
            addCounter(ctrId, ctrType, 
                    COUNTER_TABLE[ctrType].name,
                    COUNTER_TABLE[ctrType].hasDefault,
                    COUNTER_TABLE[ctrType].isInternal,
                    COUNTER_TABLE[ctrType].defaultValue,
                    COUNTER_TABLE[ctrType].otfMode);
        }

        void addCounter(uint32_t ctrId, CounterType ctrType,
                const char *name, bool hasDefault, bool isInternal,
                uint64_t defaultValue, uint32_t otfMode)
        {
            CtrTableEntry *entry = new CtrTableEntry();
            entry->type = ctrType;
            entry->name = name;
            entry->hasDefault = hasDefault;
            entry->isInternal = isInternal;
            entry->defaultValue = (uint64_t)defaultValue;
            entry->otfMode = otfMode;

            counters[ctrId] = entry;
            ctrIDs.insert(ctrId);

            maxCtrId = std::max(maxCtrId, ctrId);
        }

        CtrTableEntry* getCounter(uint32_t ctrId) const
        {
            CtrEntryMap::const_iterator iter = counters.find(ctrId);
            if (iter != counters.end())
                return iter->second;
            else
                return NULL;
        }
        
        uint32_t getCtrId(CounterType ctrType) const
        {
            for (CtrEntryMap::const_iterator iter = counters.begin();
                    iter != counters.end(); ++iter)
            {
                if (iter->second->type == ctrType)
                    return iter->first;
            }
            
            return 0;
        }

        uint32_t getNewCtrId()
        {
            return maxCtrId + 1;
        }
        
        const CtrIdSet &getAllCounterIDs() const
        {
            return ctrIDs;
        }

    private:
        uint32_t maxCtrId;
        CtrEntryMap counters;
        CtrIdSet ctrIDs;
    };
}

#endif	/* COUNTERTABLE_HPP */

