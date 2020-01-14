#include "Work_Executor.h"
#include "../Work_Reader/Work_Reader.h"
#include "../Util/Utilities.h"
#include "../Query_execution/Query_executor/Query_Executor.h"
#include <pthread.h>
#include <semaphore.h>
#include <unistd.h>

pthread_mutex_t t_m;// = PTHREAD_MUTEX_INITIALIZER;
//sem_t me;
	   
struct Args {
  pthread_t thread_id;
  Query_Ptr Current_Query;
  Table_Ptr Relations;
  FILE *fp_write;
};
 
typedef struct Args* Args_Ptr;

void *myThreadFun(void *vargp) {
  struct Args *args = (struct Args*) vargp;
  uint64_t *id = (uint64_t*)args->thread_id;
//  printf("Printing from Thread \nQuery:");
//  Print_Query(args->Current_Query);
  //sleep(1);

  pthread_mutex_lock(&t_m);
  Query_Ptr Query = Allocate_And_Copy_Query(args->Current_Query);
  Table_Ptr Relations = args->Relations;
  FILE *fp_write = args->fp_write;

  Delete_Query(args->Current_Query);
  free(args->Current_Query);
  args->Relations = NULL;
  args->fp_write = NULL;
  pthread_mutex_unlock(&t_m);

  Execute_Query(Query, Relations, fp_write);
  Delete_Query(Query);
  free(Query);

  //fprintf(args->fp_write, "\n");
  
  //printf("UNLOCK\n");
  //sem_post(&me);
    
//  pthread_exit(args->thread_id);
  return NULL;
}   


void Start_Work(Table_Ptr Relations,Argument_Data_Ptr Arg_Data){

  const char*path = construct_Path(Get_Work_FileName(Arg_Data), Get_Dir_Name(Arg_Data));
  printf("work: %s\n", path);
  FILE *fp;
  Open_File_for_Read(&fp, path);
  Batch_Ptr Current_Batch;

  pthread_t *thread_id; 
  if(pthread_mutex_init(&t_m, NULL) != 0) {
    printf("\n mutex init failed\n"); exit(1);
  }
  //sem_init(&me,1,0);

  FILE *fp_write = fopen("Results", "w");
  while((Current_Batch = Read_next_Batch(fp)) != NULL) {
    int cnt = Get_num_of_Queries(Current_Batch);
    Args_Ptr *args = (Args_Ptr*)malloc(cnt * sizeof(Args_Ptr));
    thread_id = (pthread_t*)malloc(cnt * sizeof(pthread_t));
	int i = 0;
	while(Get_num_of_Queries(Current_Batch)){
    //for(int i = 0; i < 10; i++) {
      args[i] = (Args_Ptr)malloc(sizeof(struct Args));
      args[i]->thread_id = thread_id[i];
      args[i]->Current_Query = Pop_Next_Query_from_Batch(Current_Batch);
      args[i]->Relations = Relations;
      args[i]->fp_write = fp_write;
      
	  pthread_create(&thread_id[i], NULL, myThreadFun, (void *)args[i]);
	  pthread_join(thread_id[i], NULL);

	  i++; 
      break;
    }
	//printf("\t\t\t%d\n", cnt);
    for(int i = 0; i < cnt; i++) {
      //printf("LOCK\n");
      //sem_wait(&me);
	  //free(args[i]);
    }
	free(args);
    Delete_Batch(Current_Batch);
    free(thread_id);
    break;
  }
  fclose(fp_write);

  free(path);
  fclose(fp);

}
