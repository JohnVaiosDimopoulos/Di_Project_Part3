#include "./Initializer/Argument_Manager/Argument_Manager.h"
#include "./Initializer/Table_Allocator/Table_Allocator.h"
#include "./Work_Executor/Work_Executor.h"
#include <time.h>
#include <unistd.h>



int main(int argc, char** argv){
  struct timespec start,finish;
  double time_elapsed;
  Arg_Manager_Ptr Manager = Create_ArgManager(argc,argv);
  Argument_Data_Ptr Arg_Data = Get_Argument_Data(Manager);
  Table_AllocatorPtr Table_Allocator = Create_Table_Allocator(Arg_Data);
  Table_Ptr Table = Allocate_Table(Table_Allocator);
  Fill_Table(Table, Table_Allocator);
  printf("DONE READING DATA\n");


  clock_gettime(CLOCK_MONOTONIC,&start);

  Start_Work(Table, Arg_Data);

  clock_gettime(CLOCK_MONOTONIC,&finish);

  time_elapsed = (finish.tv_sec -start.tv_sec);
  time_elapsed += (finish.tv_nsec - start.tv_nsec)/1000000000.0;
  printf("TOTAL TIME: %f\n",time_elapsed);

  Delete_ArgManager(Manager);
  Delete_Argument_Data(Arg_Data);
  Delete_Table_Allocator(Table_Allocator);
  Delete_Table(Table);

  return 0;

}

