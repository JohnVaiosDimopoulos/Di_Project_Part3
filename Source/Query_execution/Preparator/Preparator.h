#ifndef MULTI_JOIN_PREPARATOR_H
#define MULTI_JOIN_PREPARATOR_H

#include "../Query_parser/Query_parser.h"
#include "../Execution_Queue/Execution_Queue.h"

typedef struct Rel_Queue* Rel_Queue_Ptr;

Execution_Queue_Ptr Prepare_Execution_Queue(Parsed_Query_Ptr Parsed_Query);
Rel_Queue_Ptr Prepare_Rel_Queue(Parsed_Query_Ptr );


#endif //MULTI_JOIN_PREPARATOR_H
