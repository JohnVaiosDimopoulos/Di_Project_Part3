#ifndef MULTI_JOIN_PROJECTION_EXECUTOR_H
#define MULTI_JOIN_PROJECTION_EXECUTOR_H

#include "../../Query_parser/Query_parser.h"
#include "../../../Initializer/Table_Allocator/Table_Allocator.h"
#include "../Join_Execution/Join_Execution.h"


void Execute_Projections(Intermediate_Result_Ptr Res, Parsed_Query_Ptr, Table_Ptr, FILE*);

#endif
