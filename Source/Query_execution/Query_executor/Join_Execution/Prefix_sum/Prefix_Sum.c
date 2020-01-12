#include <stdlib.h>
#include "../Histogram/Histogram.h"
#include "Prefix_Sum.h"


struct Psum{
  int Array[PSUM_SIZE];
};

static void Fill_Psum(Histogram_Ptr Histogram, Psum_Ptr Psum){

  int current_addr=0;
  int* Hist_array = Get_Histogram_Array(Histogram);

  for(int i =0;i<HISTOGRAM_SIZE;i++){
    //if the value is 0 the current byte is not present
    if(Hist_array[i]==0)
      continue;
    Psum->Array[i] = current_addr;
    //set the starting addr of the next byte that is present
    current_addr+=Hist_array[i];
  }
}

static Psum_Ptr Create_and_Initialize_Psum() {

  Psum_Ptr Psum = (Psum_Ptr)malloc(sizeof(struct Psum));
  for(int i =0;i<PSUM_SIZE; i++){
    // all the bytes that are not present will have a value of -1;
    Psum->Array[i]=-1;
  }
  return Psum;
}


Psum_Ptr Get_Psum(Histogram_Ptr Histogram) {
  Psum_Ptr Psum = Create_and_Initialize_Psum();
  Fill_Psum(Histogram,Psum);
  return Psum;
}

void Print_Psum(Psum_Ptr Psum){
  for(int i =0; i < PSUM_SIZE; i++){
    if(Psum->Array[i]==-1)
      continue;
    printf("value:%d  psum:%d\n",i,Psum->Array[i]);
  }
  printf("\n\n");
}

int* Get_psum_Array(Psum_Ptr Psum){
  return Psum->Array;
}

void Delete_Psum(Psum_Ptr Psum){
  free(Psum);
}

