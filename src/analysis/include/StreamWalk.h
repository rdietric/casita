/*
 * This file is part of the CASITA software
 *
 * Copyright (c) 2018,
 * Technische Universitaet Dresden, Germany
 *
 * This software may be modified and distributed under the terms of
 * a BSD-style license. See the COPYING file in the package base
 * directory for details.
 *
 */

#pragma once

namespace casita
{
  typedef struct
  {
    GraphNode::GraphNodeList list; //<! list of nodes for the stream walk
    uint64_t waitStateTime;        //<! accumulated waiting time of the list members, end node waiting time is not included
    AnalysisEngine* analysis;
  } StreamWalkInfo;
}
