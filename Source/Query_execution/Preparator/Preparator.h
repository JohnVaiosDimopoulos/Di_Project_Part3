#ifndef MULTI_JOIN_PREPARATOR_H
#define MULTI_JOIN_PREPARATOR_H

#include "../Query_parser/Query_parser.h"
#include "../Execution_Queue/Execution_Queue.h"
#include "../../Initializer/Table_Allocator/Table_Allocator.h"
#include "../../Util/Utilities.h"

typedef struct Rel_Queue* Rel_Queue_Ptr;
typedef struct Stats* Stats_Ptr;

Execution_Queue_Ptr Prepare_Execution_Queue(Parsed_Query_Ptr Parsed_Query, Table_Ptr);
Rel_Queue_Ptr Prepare_Rel_Queue(Parsed_Query_Ptr, Table_Ptr);


#endif //MULTI_JOIN_PREPARATOR_H
