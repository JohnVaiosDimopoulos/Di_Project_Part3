#include "../Argument_Data/Arguments_Data.h"
#ifndef SORT_JOIN_ARGUMET_MANAGER_H
#define SORT_JOIN_ARGUMET_MANAGER_H

typedef struct Arg_Manager* Arg_Manager_Ptr;
Argument_Data_Ptr Get_Argument_Data(Arg_Manager_Ptr);
Arg_Manager_Ptr Create_ArgManager(int, char**);
void Delete_ArgManager(Arg_Manager_Ptr);

#endif //SORT_JOIN_ARGUMET_MANAGER_H
