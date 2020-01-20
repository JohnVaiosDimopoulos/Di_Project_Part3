#include "Work_Executor.h"
#include "../Work_Reader/Work_Reader.h"
#include "../Util/Utilities.h"
#include "../Query_execution/Query_executor/Query_Executor.h"
#include <pthread.h>
#include <semaphore.h>
#include <unistd.h>

#define LIMIT 13

int end_of_batch = 0;
int end = 0;
int alive_threads = 0;
pthread_mutex_t m = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t c = PTHREAD_COND_INITIALIZER;
sem_t main_thread;
sem_t thread;
	   
struct Args {
  pthread_t thread_id;
  Query_Ptr Current_Query;
  Table_Ptr Relations;
  FILE *fp_write;
  uint64_t **Results_array;
  int query_id;
};
 
typedef struct Args* Args_Ptr;

Args_Ptr args;

void *myThreadFun(void *vargp) {
  while(1) {
    //Wait for next query
    sem_wait(&thread);
    pthread_t id;
	if(end) {
      pthread_exit(id);
	  break;
	}
	//printf("JOB...\n");
    //struct Args *args = (struct Args*) vargp;
    id = args->thread_id;
    Query_Ptr Query = Allocate_And_Copy_Query(args->Current_Query);
    Table_Ptr Relations = args->Relations;
    FILE *fp_write = args->fp_write;
    uint64_t **Results_array = args->Results_array;
    int query_id = args->query_id;
	//printf("Printing from Thread \nQuery:");
    //Print_Query(Query);
    printf("\n");
  
    Delete_Query(args->Current_Query);
    free(args->Current_Query);
    args->Relations = NULL;
    args->fp_write = NULL;
    args->Results_array = NULL;
  
    sem_post(&main_thread);
  
    Execute_Query(Query, Relations, fp_write, Results_array, query_id);
    Delete_Query(Query);
    free(Query);
  
    pthread_mutex_lock(&m);
    alive_threads--;
    //printf("---------->%d\n", alive_threads);
    pthread_cond_signal(&c);
    pthread_mutex_unlock(&m);
  }
}   

static void Wait_for_available_thread() {
  pthread_mutex_lock(&m);
  while(alive_threads >= LIMIT)
    pthread_cond_wait(&c, &m);
  pthread_mutex_unlock(&m);
}

void Start_Work(Table_Ptr Relations,Argument_Data_Ptr Arg_Data){

  const char*path = construct_Path(Get_Work_FileName(Arg_Data), Get_Dir_Name(Arg_Data));
  printf("work: %s\n", path);
  FILE *fp;
  Open_File_for_Read(&fp, path);
  Batch_Ptr Current_Batch;

  pthread_t *thread_id; 
  sem_init(&thread, 1, 0);
  sem_init(&main_thread, 1, 0);

  FILE *fp_write = fopen("Results", "w");
  uint64_t *Results_array[4];
  args = (Args_Ptr)malloc(sizeof(struct Args));
  thread_id = (pthread_t*)malloc(LIMIT * sizeof(pthread_t));
  for(int i = 0; i < LIMIT; i++)
    pthread_create(&thread_id[i], NULL, myThreadFun, (void *)args);

  while((Current_Batch = Read_next_Batch(fp)) != NULL) {

	//Array for printing query-results in the right order
    int num_of_queries = Get_num_of_Queries(Current_Batch);
	for(int i = 0; i < 4; i++) {
	  Results_array[i] = (uint64_t*)malloc(num_of_queries * sizeof(uint64_t));
	  for(int j = 0; j < num_of_queries; j++)
	    Results_array[i][j] = -1;
	}

	int query_id = 0;
	while(Get_num_of_Queries(Current_Batch)){

      //Fill argument-struct for pthread_create
      args->thread_id = thread_id[query_id];
      args->Current_Query = Pop_Next_Query_from_Batch(Current_Batch);
      args->Relations = Relations;
      args->fp_write = fp_write;
      args->Results_array = Results_array;
      args->query_id = query_id;
     
      //Wait for available thread
      Wait_for_available_thread();
      pthread_mutex_lock(&m);
      alive_threads++;
      //printf("----->%d\n", alive_threads);
      pthread_mutex_unlock(&m);

	  //Wake thread
	  //printf("WAKE UP THREAD\n");
      Print_Query(args->Current_Query);
      sem_post(&thread);
	  //Wait to copy the arguments
      sem_wait(&main_thread);
	  //printf("THREAD GOT IT\n");

	  query_id++; 
//      break;
    }
	while(alive_threads);
	printf("\n\n\nEND OF BATCH\n");
    for(int i = 0; i < num_of_queries; i++) {
      for(int j = 0; j < 4; j++) {
        if(Results_array[j][i] != -1)
          fprintf(fp_write, "\t-------------------->%d ", Results_array[j][i]);
      }
      fprintf(fp_write, "\n");
	}
    Delete_Batch(Current_Batch);
	for(int i = 0; i < 4; i++)
      free(Results_array[i]);
//    break;
  }
  //Wake all threads
  for(int i = 0; i < LIMIT; i++) {
    end = 1;
    sem_post(&thread);
  }
  for(int i = 0; i < LIMIT; i++) {
    pthread_join(thread_id[i], NULL);
  }

  free(args);
  free(thread_id);
  fclose(fp_write);
  free(path);
  fclose(fp);
}
