#include "Work_Executor.h"
#include "../Work_Reader/Work_Reader.h"
#include "../Util/Utilities.h"
#include "../Query_execution/Query_executor/Query_Executor.h"
#include <pthread.h>
#include <unistd.h>


pthread_mutex_t m;// = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t t_m;// = PTHREAD_MUTEX_INITIALIZER;

struct Args {
  pthread_t thread_id; 
  Query_Ptr Current_Query;
  Table_Ptr Relations;
  FILE *fp_write;
};

void *myThreadFun(void *vargp) { 
	pthread_mutex_lock(&t_m);
	struct Args *args = (struct Args*) vargp; 
    printf("Printing from Thread %lu,\nQuery:", *args->thread_id); 
	Print_Query(args->Current_Query);
	sleep(1);
    //Execute_Query(args->Current_Query, args->Relations, args->fp_write);
    //fprintf(args->fp_write, "\n");
   // Delete_Query(args->Current_Query);
   // free(args->Current_Query);
	pthread_mutex_unlock(&t_m);

//	pthread_exit(args->thread_id);
	return NULL;
} 
   

void Start_Work(Table_Ptr Relations,Argument_Data_Ptr Arg_Data){

  const char*path = construct_Path(Get_Work_FileName(Arg_Data), Get_Dir_Name(Arg_Data));
  printf("work: %s\n", path);
  FILE *fp;
  Open_File_for_Read(&fp, path);
  Batch_Ptr Current_Batch;
  Query_Ptr Current_Query;

  FILE *fp_write = fopen("Results", "w");
  pthread_t thread_id; 

  if(pthread_mutex_init(&m, NULL) != 0) {
    printf("\n mutex init failed\n"); exit(1);
  }
  if(pthread_mutex_init(&t_m, NULL) != 0) {
    printf("\n mutex init failed\n"); exit(1);
  }


  while((Current_Batch = Read_next_Batch(fp)) != NULL) {
	//while(Get_num_of_Queries(Current_Batch)){
	for(int i = 0; i < 2; i++) {
	 
	  pthread_mutex_lock(&m);
      printf("LOCK\n"); 
      struct Args args;
	  Current_Query = Pop_Next_Query_from_Batch(Current_Batch);

      //Execute_Query(Current_Query, Relations, fp_write);
      //printf("Before Thread\n"); 
      args.thread_id = thread_id;
      args.Current_Query = Current_Query;
      args.Relations = Relations;
      args.fp_write = fp_write;

      //fprintf(fp_write, "\n");
      //Delete_Query(Current_Query);
      //free(Current_Query);

      pthread_create(&thread_id, NULL, myThreadFun, (void *)&args); 
      printf("UNLOCK\n"); 
	  pthread_mutex_unlock(&m);
      //pthread_join(thread_id, NULL); 
      //printf("After Thread\n\n"); 


	  //this break and the next one should not be here
	  //just for checking
//	  break;
    }
    Delete_Batch(Current_Batch);
	break;
  }
  //pthread_mutex_destroy(&t_m);
  pthread_mutex_destroy(&m);

  fclose(fp_write);

  free(path);
  fclose(fp);

}
