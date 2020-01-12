#ifndef MULTI_JOIN_WORK_READER_H
#define MULTI_JOIN_WORK_READER_H

#include "../Initializer/Argument_Data/Arguments_Data.h"
#include "../Initializer/Table_Allocator/Table_Allocator.h"
#include "../Batch/Batch.h"
#include "../Query/Query.h"
#include <stdlib.h>
#include <stdio.h>

Batch_Ptr Read_next_Batch(FILE*);

#endif
