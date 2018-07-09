#include <jni.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/shm.h>
#include <sys/ipc.h>
#include <sys/types.h>
#include <errno.h>
#include "org_apache_storm_topology_accelerate_NativeBufferManager.h"

const char* INTTYPE = "int";
const char* SHORTTYPE = "short";
const char* LONGTYPE = "long";
const char* DOUBLETYPE = "double";
const char* FLOATTYPE = "float";
const char* CHARTYPE = "char";
const char* BYTETYPE = "byte";
const char* BOOLEANTYPE = "boolean";
const char* INPUT_AND_OUTPUT_FLAG_TYPE = "INPUT_AND_OUTPUT_FLAG_TYPE";

const int INPUT_DATA_READY = 1;
const int INPUT_DATA_CONSUMED = 0;
const int INPUT_DATA_END = -1;

const int OUTPUT_DATA_READY = 1;
const int OUTPUT_DATA_CONSUMED = 0;

struct shared_data_flag {
     int input_flag;
     int output_flag;
};
/*
 * Class:     org_apache_storm_topology_accelerate_NativeBufferManager
 * Method:    shmGet
 * Signature: (IILjava/lang/String;)I
 */
JNIEXPORT jint JNICALL Java_org_apache_storm_topology_accelerate_NativeBufferManager_shmGet
  (JNIEnv * env, jobject obj, jint size, jstring data_type){
         int data_type_size=0;
         const char *data_type_str;
         data_type_str = (const char *)env->GetStringUTFChars(data_type,NULL);
         if(data_type_str == NULL){
             fprintf(stderr,"get data type failed\n");
             exit(EXIT_FAILURE);
         }
         if(strcmp(data_type_str,INPUT_AND_OUTPUT_FLAG_TYPE) == 0){
              data_type_size = sizeof(struct shared_data_flag);
         }else if(strcmp(data_type_str,INTTYPE) == 0){
             data_type_size = sizeof(int);
         }else if(strcmp(data_type_str,SHORTTYPE) == 0){
             data_type_size = sizeof(short);
         }else if(strcmp(data_type_str,LONGTYPE) == 0){
             data_type_size = sizeof(long);
         }else if(strcmp(data_type_str,DOUBLETYPE) == 0){
             data_type_size = sizeof(double);
         }else if(strcmp(data_type_str,FLOATTYPE) == 0){
             data_type_size = sizeof(float);
         }else if(strcmp(data_type_str,CHARTYPE) == 0){
             data_type_size = sizeof(char);
         }else if(strcmp(data_type_str,BYTETYPE) == 0){
             data_type_size = sizeof(char);
         }else if(strcmp(data_type_str,BOOLEANTYPE) == 0){
             data_type_size = sizeof(bool);
         }else{
             fprintf(stderr,"data_type is invalid\n");
             exit(EXIT_FAILURE);
         }
         jint shmid = shmget(IPC_PRIVATE,data_type_size * size,0666|IPC_CREAT);
         if(shmid == -1){
             fprintf(stderr,"shmget failed! info: %s\n",strerror(errno));
             exit(EXIT_FAILURE);
         }
         // 初始化data_flag的值
         if(strcmp(data_type_str,INPUT_AND_OUTPUT_FLAG_TYPE) == 0){
             void * shared_memory = shmat(shmid,(void *)0,0);
             struct shared_data_flag * data_flag = (struct shared_data_flag * )shared_memory;
             data_flag->input_flag = INPUT_DATA_CONSUMED;
             data_flag->output_flag = OUTPUT_DATA_CONSUMED;
             if(shmdt(shared_memory) == -1){
                fprintf(stderr,"shmdt failed\n");
                env->ReleaseStringUTFChars(data_type,data_type_str);
                exit(EXIT_FAILURE);
             }
         }
         env->ReleaseStringUTFChars(data_type,data_type_str);
         return shmid;
  }
/*
 * Class:     org_apache_storm_topology_accelerate_NativeBufferManager
 * Method:    shmClear
 * Signature: ([I)V
 *//*
JNIEXPORT void JNICALL Java_org_apache_storm_topology_accelerate_NativeBufferManager_shmClear
  (JNIEnv * env, jobject obj, jintArray shmid_array){
         jint len = env->GetArrayLength(shmid_array);
         jint* shmids = env->GetIntArrayElements(shmid_array,NULL);
         if(shmids == NULL){
             fprintf(stderr,"get shmid array failed\n");
             exit(EXIT_FAILURE);
         }
         for(int i = 0;i<len;i++){
            if(shmctl(shmids[i],IPC_RMID,0) == -1){
                   fprintf(stderr,"shmctl(IPC_RMID) failed\n");
                   exit(EXIT_FAILURE);
            }
         }
         env->ReleaseIntArrayElements(shmid_array,shmids,0);
  }*/
/*
 * Class:     org_apache_storm_topology_accelerate_NativeBufferManager
 * Method:    putIntToNativeShm
 * Signature: (I[II)Z
 */
JNIEXPORT jboolean JNICALL Java_org_apache_storm_topology_accelerate_NativeBufferManager_putIntToNativeShm
  (JNIEnv * env, jobject obj, jint shmid, jintArray array, jint size){
         void * shared_memory = shmat(shmid,(void *)0,0);
         int* shared_stuff = (int *)shared_memory;
         jint* arr = env->GetIntArrayElements(array,NULL);
         if(arr == NULL){
             return JNI_FALSE;
         }
         for(int i = 0;i<size;i++){
             *(shared_stuff+i) = arr[i];
         }
         if(shmdt(shared_memory) == -1){
              fprintf(stderr,"shmdt failed\n");
              return JNI_FALSE;
         }
         env->ReleaseIntArrayElements(array,arr,0);
         return JNI_TRUE;
  }
/*
 * Class:     org_apache_storm_topology_accelerate_NativeBufferManager
 * Method:    putLongToNativeShm
 * Signature: (I[JI)Z
 */
JNIEXPORT jboolean JNICALL Java_org_apache_storm_topology_accelerate_NativeBufferManager_putLongToNativeShm
  (JNIEnv * env, jobject obj, jint shmid, jlongArray array, jint size){
         void * shared_memory = shmat(shmid,(void *)0,0);
         long* shared_stuff = (long *)shared_memory;
         jlong* arr = env->GetLongArrayElements(array,NULL);
         if(arr == NULL){
             return JNI_FALSE;
         }
         for(int i = 0;i<size;i++){
             *(shared_stuff+i) = arr[i];
         }
         if(shmdt(shared_memory) == -1){
              fprintf(stderr,"shmdt failed\n");
              return JNI_FALSE;
         }
         env->ReleaseLongArrayElements(array,arr,0);
         return JNI_TRUE;
  }
/*
 * Class:     org_apache_storm_topology_accelerate_NativeBufferManager
 * Method:    putShortToNativeShm
 * Signature: (I[SI)Z
 */
JNIEXPORT jboolean JNICALL Java_org_apache_storm_topology_accelerate_NativeBufferManager_putShortToNativeShm
  (JNIEnv * env, jobject obj, jint shmid, jshortArray array, jint size){
         void * shared_memory = shmat(shmid,(void *)0,0);
         short* shared_stuff = (short *)shared_memory;
         jshort* arr = env->GetShortArrayElements(array,NULL);
         if(arr == NULL){
             return JNI_FALSE;
         }
         for(int i = 0; i<size;i++){
             *(shared_stuff+i) = arr[i];
         }
         if(shmdt(shared_memory) == -1){
              fprintf(stderr,"shmdt failed\n");
              return JNI_FALSE;
         }
         env->ReleaseShortArrayElements(array,arr,0);
         return JNI_TRUE;
  }
/*
 * Class:     org_apache_storm_topology_accelerate_NativeBufferManager
 * Method:    putByteToNativeShm
 * Signature: (I[BI)Z
 */
JNIEXPORT jboolean JNICALL Java_org_apache_storm_topology_accelerate_NativeBufferManager_putByteToNativeShm
  (JNIEnv * env, jobject obj, jint shmid, jbyteArray array, jint size){
         void * shared_memory = shmat(shmid,(void *)0,0);
         char* shared_stuff = (char *)shared_memory; // 这里的指针是否需要换成char*类型？
         jbyte* arr = env->GetByteArrayElements(array,NULL);
         if(arr == NULL){
             return JNI_FALSE;
         }
         for(int i = 0; i<size;i++){
             *(shared_stuff+i) = arr[i];
         }
         if(shmdt(shared_memory) == -1){
              fprintf(stderr,"shmdt failed\n");
              return JNI_FALSE;
         }
         env->ReleaseByteArrayElements(array,arr,0);
         return JNI_TRUE;
  }

/*
 * Class:     org_apache_storm_topology_accelerate_NativeBufferManager
 * Method:    putCharToNativeShm
 * Signature: (I[CI)Z
 */
JNIEXPORT jboolean JNICALL Java_org_apache_storm_topology_accelerate_NativeBufferManager_putCharToNativeShm
  (JNIEnv * env, jobject obj, jint shmid, jcharArray array, jint size){
         void * shared_memory = shmat(shmid,(void *)0,0);
         char* shared_stuff = (char *)shared_memory;
         jchar* arr = env->GetCharArrayElements(array,NULL);
         if(arr == NULL){
             return JNI_FALSE;
         }
         for(int i = 0;i<size;i++){
             *(shared_stuff + i) = arr[i];
         }
         if(shmdt(shared_memory) == -1){
              fprintf(stderr,"shmdt failed\n");
              return JNI_FALSE;
         }
         env->ReleaseCharArrayElements(array,arr,0);
         return JNI_TRUE;
   }

/*
 * Class:     org_apache_storm_topology_accelerate_NativeBufferManager
 * Method:    putBooleanToNativeShm
 * Signature: (I[ZI)Z
 */
JNIEXPORT jboolean JNICALL Java_org_apache_storm_topology_accelerate_NativeBufferManager_putBooleanToNativeShm
  (JNIEnv * env, jobject obj, jint shmid, jbooleanArray array, jint size){
         void * shared_memory = shmat(shmid,(void *)0,0);
         bool* shared_stuff = (bool *)shared_memory;
         jboolean* arr = env->GetBooleanArrayElements(array,NULL);
         if(arr == NULL){
             return JNI_FALSE;
         }
         for(int i = 0;i<size;i++){
             *(shared_stuff + i) = arr[i];
         }
         if(shmdt(shared_memory) == -1){
              fprintf(stderr,"shmdt failed\n");
              return JNI_FALSE;
         }
         env->ReleaseBooleanArrayElements(array,arr,0);
         return JNI_TRUE;
  }

/*
 * Class:     org_apache_storm_topology_accelerate_NativeBufferManager
 * Method:    putFloatToNativeShm
 * Signature: (I[FI)Z
 */
JNIEXPORT jboolean JNICALL Java_org_apache_storm_topology_accelerate_NativeBufferManager_putFloatToNativeShm
  (JNIEnv * env, jobject obj, jint shmid, jfloatArray array, jint size){
         void * shared_memory = shmat(shmid,(void *)0,0);
         float* shared_stuff = (float *)shared_memory;
         jfloat* arr = env->GetFloatArrayElements(array,NULL);
         if(arr == NULL){
             return JNI_FALSE;
         }
         for(int i = 0;i<size;i++){
             *(shared_stuff + i) = arr[i];
         }
         if(shmdt(shared_memory) == -1){
              fprintf(stderr,"shmdt failed\n");
              return JNI_FALSE;
         }
         env->ReleaseFloatArrayElements(array,arr,0);
         return JNI_TRUE;
  }

/*
 * Class:     org_apache_storm_topology_accelerate_NativeBufferManager
 * Method:    putDoubleToNativeShm
 * Signature: (I[DI)Z
 */
JNIEXPORT jboolean JNICALL Java_org_apache_storm_topology_accelerate_NativeBufferManager_putDoubleToNativeShm
  (JNIEnv * env, jobject obj, jint shmid, jdoubleArray array, jint size){
         void * shared_memory = shmat(shmid,(void *)0,0);
         double* shared_stuff = (double *)shared_memory;
         jdouble* arr = env->GetDoubleArrayElements(array,NULL);
         if(arr == NULL){
             return JNI_FALSE;
         }
         for(int i = 0;i<size;i++){
             *(shared_stuff + i) = arr[i];
         }
         if(shmdt(shared_memory) == -1){
              fprintf(stderr,"shmdt failed\n");
              return JNI_FALSE;
         }
         env->ReleaseDoubleArrayElements(array,arr,0);
         return JNI_TRUE;
  }
/*
 * Class:     org_apache_storm_topology_accelerate_NativeBufferManager
 * Method:    getIntFromNativeShm
 * Signature: (I[II)V
 */
JNIEXPORT void JNICALL Java_org_apache_storm_topology_accelerate_NativeBufferManager_getIntFromNativeShm
  (JNIEnv * env, jobject obj, jint shmid, jintArray array, jint size){
         void * shared_memory = shmat(shmid,(void *)0,0);
         int* shared_stuff = (int *)shared_memory;
         env->SetIntArrayRegion(array,0,size,(jint *)shared_stuff);
         if(shmdt(shared_memory) == -1){
              fprintf(stderr,"shmdt failed\n");
              exit(EXIT_FAILURE);
         }
  }

/*
 * Class:     org_apache_storm_topology_accelerate_NativeBufferManager
 * Method:    getLongFromNativeShm
 * Signature: (I[JI)V
 */
JNIEXPORT void JNICALL Java_org_apache_storm_topology_accelerate_NativeBufferManager_getLongFromNativeShm
  (JNIEnv * env, jobject obj, jint shmid, jlongArray array, jint size){
         void * shared_memory = shmat(shmid,(void *)0,0);
         long * shared_stuff = (long *)shared_memory;
         env->SetLongArrayRegion(array,0,size,(jlong *)shared_stuff);
         if(shmdt(shared_memory) == -1){
              fprintf(stderr,"shmdt failed\n");
              exit(EXIT_FAILURE);
         }
  }

/*
 * Class:     org_apache_storm_topology_accelerate_NativeBufferManager
 * Method:    getShortFromNativeShm
 * Signature: (I[SI)V
 */
JNIEXPORT void JNICALL Java_org_apache_storm_topology_accelerate_NativeBufferManager_getShortFromNativeShm
  (JNIEnv * env, jobject obj, jint shmid, jshortArray array, jint size){
         void * shared_memory = shmat(shmid,(void *)0,0);
         short * shared_stuff = (short *)shared_memory;
         env->SetShortArrayRegion(array,0,size,(jshort *)shared_stuff);
         if(shmdt(shared_memory) == -1){
              fprintf(stderr,"shmdt failed\n");
              exit(EXIT_FAILURE);
         }
  }

/*
 * Class:     org_apache_storm_topology_accelerate_NativeBufferManager
 * Method:    getByteFromNativeShm
 * Signature: (I[BI)V
 */
JNIEXPORT void JNICALL Java_org_apache_storm_topology_accelerate_NativeBufferManager_getByteFromNativeShm
  (JNIEnv * env, jobject obj, jint shmid, jbyteArray array, jint size){
         void * shared_memory = shmat(shmid,(void *)0,0);
         char * shared_stuff = (char *)shared_memory;
         env->SetByteArrayRegion(array,0,size,(jbyte *)shared_stuff);
         if(shmdt(shared_memory) == -1){
              fprintf(stderr,"shmdt failed\n");
              exit(EXIT_FAILURE);
         }
  }

/*
 * Class:     org_apache_storm_topology_accelerate_NativeBufferManager
 * Method:    getCharacterFormNativeShm
 * Signature: (I[CI)V
 */
JNIEXPORT void JNICALL Java_org_apache_storm_topology_accelerate_NativeBufferManager_getCharFromNativeShm
  (JNIEnv * env, jobject obj, jint shmid, jcharArray array, jint size){
         void * shared_memory = shmat(shmid,(void *)0,0);
         char * shared_stuff = (char *)shared_memory;
         jchar * arr = env->GetCharArrayElements(array,JNI_FALSE);
         for(int i = 0; i<size;i++){
              *(arr+i) = *(shared_stuff+i);
         }
         env->ReleaseCharArrayElements(array,arr,0);
         if(shmdt(shared_memory) == -1){
              fprintf(stderr,"shmdt failed\n");
              exit(EXIT_FAILURE);
         }
  }

/*
 * Class:     org_apache_storm_topology_accelerate_NativeBufferManager
 * Method:    getBooleanFromNativeShm
 * Signature: (I[ZI)V
 */
JNIEXPORT void JNICALL Java_org_apache_storm_topology_accelerate_NativeBufferManager_getBooleanFromNativeShm
  (JNIEnv * env, jobject obj, jint shmid, jbooleanArray array, jint size){
         void * shared_memory = shmat(shmid,(void *)0,0);
         bool * shared_stuff = (bool *)shared_memory;
         env->SetBooleanArrayRegion(array,0,size,(jboolean *)shared_stuff);
         if(shmdt(shared_memory) == -1){
              fprintf(stderr,"shmdt failed\n");
              exit(EXIT_FAILURE);
         }
  }

/*
 * Class:     org_apache_storm_topology_accelerate_NativeBufferManager
 * Method:    getFloatFromNativeShm
 * Signature: (I[FI)V
 */
JNIEXPORT void JNICALL Java_org_apache_storm_topology_accelerate_NativeBufferManager_getFloatFromNativeShm
  (JNIEnv * env, jobject obj, jint shmid, jfloatArray array, jint size){
         void * shared_memory = shmat(shmid,(void *)0,0);
         float * shared_stuff = (float *)shared_memory;
         env->SetFloatArrayRegion(array,0,size,(jfloat *)shared_stuff);
         if(shmdt(shared_memory) == -1){
              fprintf(stderr,"shmdt failed\n");
              exit(EXIT_FAILURE);
         }
  }

/*
 * Class:     org_apache_storm_topology_accelerate_NativeBufferManager
 * Method:    getDoubleFromNativeShm
 * Signature: (I[DI)V
 */
JNIEXPORT void JNICALL Java_org_apache_storm_topology_accelerate_NativeBufferManager_getDoubleFromNativeShm
  (JNIEnv * env, jobject obj, jint shmid, jdoubleArray array, jint size){
         void * shared_memory = shmat(shmid,(void *)0,0);
         double * shared_stuff = (double *)shared_memory;
         env->SetDoubleArrayRegion(array,0,size,(jdouble *)shared_stuff);
         if(shmdt(shared_memory) == -1){
              fprintf(stderr,"shmdt failed\n");
              exit(EXIT_FAILURE);
         }
  }

/*
 * Class:     org_apache_storm_topology_accelerate_NativeBufferManager
 * Method:    setInputDataReady
 * Signature: (I)V
 */
JNIEXPORT void JNICALL Java_org_apache_storm_topology_accelerate_NativeBufferManager_setInputDataReady
  (JNIEnv * env, jobject obj, jint shmid){
         void * shared_memory = shmat(shmid,(void *)0,0);
         struct shared_data_flag * shared_data_flag = (struct shared_data_flag *)shared_memory;
         shared_data_flag->input_flag = INPUT_DATA_READY;
         if(shmdt(shared_memory) == -1){
              fprintf(stderr,"shmdt failed\n");
              exit(EXIT_FAILURE);
         }
  }

/*
 * Class:     org_apache_storm_topology_accelerate_NativeBufferManager
 * Method:    waitOutputDataReady
 * Signature: (I)V
 */
JNIEXPORT void JNICALL Java_org_apache_storm_topology_accelerate_NativeBufferManager_waitOutputDataReady
  (JNIEnv * env, jobject obj, jint shmid){
        void * shared_memory = shmat(shmid,(void *)0,0);
        struct shared_data_flag * shared_data_flag = (struct shared_data_flag *)shared_memory;
        while(shared_data_flag->output_flag != OUTPUT_DATA_READY){ }
        if(shmdt(shared_memory) == -1){
             fprintf(stderr,"shmdt failed\n");
             exit(EXIT_FAILURE);
        }
  }

/*
 * Class:     org_apache_storm_topology_accelerate_NativeBufferManager
 * Method:    setOutputDataConsumed
 * Signature: (I)V
 */
JNIEXPORT void JNICALL Java_org_apache_storm_topology_accelerate_NativeBufferManager_setOutputDataConsumed
  (JNIEnv * env, jobject obj, jint shmid){
        void * shared_memory = shmat(shmid,(void *)0,0);
        struct shared_data_flag * shared_data_flag = (struct shared_data_flag *)shared_memory;
        shared_data_flag->output_flag = OUTPUT_DATA_CONSUMED;
        if(shmdt(shared_memory) == -1){
             fprintf(stderr,"shmdt failed\n");
             exit(EXIT_FAILURE);
        }
  }

/*
 * Class:     org_apache_storm_topology_accelerate_NativeBufferManager
 * Method:    setInputDataEnd
 * Signature: (I)V
 */
JNIEXPORT void JNICALL Java_org_apache_storm_topology_accelerate_NativeBufferManager_setInputDataEnd
  (JNIEnv * env, jobject obj, jint shmid){
        void * shared_memory = shmat(shmid,(void *)0,0);
        struct shared_data_flag * shared_data_flag = (struct shared_data_flag *)shared_memory;
        shared_data_flag->input_flag = INPUT_DATA_END;
        if(shmdt(shared_memory) == -1){
             fprintf(stderr,"shmdt failed\n");
             exit(EXIT_FAILURE);
        }
  }

/*
 * Class:     org_apache_storm_topology_accelerate_NativeBufferManager
 * Method:    waitInputDataConsumed
 * Signature: (I)V
 */
JNIEXPORT void JNICALL Java_org_apache_storm_topology_accelerate_NativeBufferManager_waitInputDataConsumed
  (JNIEnv * env, jobject obj, jint shmid){
        void * shared_memory = shmat(shmid,(void *)0,0);
        struct shared_data_flag * shared_data_flag = (struct shared_data_flag *)shared_memory;
        while(shared_data_flag->input_flag != INPUT_DATA_CONSUMED){ }
        if(shmdt(shared_memory) == -1){
             fprintf(stderr,"shmdt failed\n");
             exit(EXIT_FAILURE);
        }
  }
