#ifndef MULTI_JOIN_FILTER_EXECUTOR_H
#define MULTI_JOIN_FILTER_EXECUTOR_H

#include "stdint.h"
#include "../../Query_parser/Query_parser.h"
#include "../../../Initializer/Table_Allocator/Table_Allocator.h"






//Filters_Outcome_Ptr Execute_Filters(Table_Ptr, Parsed_Query_Ptr, int *, int);
void Execute_Filters(Table_Ptr, Parsed_Query_Ptr);



#endif //MULTI_JOIN_FILTER_EXECUTOR_H
