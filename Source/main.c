#include "./Initializer/Argument_Manager/Argument_Manager.h"
#include "./Initializer/Table_Allocator/Table_Allocator.h"
#include "./Work_Executor/Work_Executor.h"
#include <time.h>
#include <unistd.h>



int main(int argc, char** argv){
  clock_t start,end;
  Arg_Manager_Ptr Manager = Create_ArgManager(argc,argv);
  Argument_Data_Ptr Arg_Data = Get_Argument_Data(Manager);


  Table_AllocatorPtr Table_Allocator = Create_Table_Allocator(Arg_Data);
  Table_Ptr Table = Allocate_Table(Table_Allocator);
  Fill_Table(Table, Table_Allocator);

  start = clock();

  Start_Work(Table, Arg_Data);

  end = clock();
  double cpu_time_used = (((double) (end - start)) / CLOCKS_PER_SEC);
  printf("TOTAL TIME: %f\n",cpu_time_used);

  Delete_ArgManager(Manager);
  Delete_Argument_Data(Arg_Data);
  Delete_Table_Allocator(Table_Allocator);
  Delete_Table(Table);

  return 0;

}

