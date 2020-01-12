#ifndef MULTI_JOIN_PREPARATOR_H
#define MULTI_JOIN_PREPARATOR_H

#include "../Query_parser/Query_parser.h"
#include "../Execution_Queue/Execution_Queue.h"

Execution_Queue_Ptr Prepare_Execution_Queue(Parsed_Query_Ptr Parsed_Query);

#endif //MULTI_JOIN_PREPARATOR_H
