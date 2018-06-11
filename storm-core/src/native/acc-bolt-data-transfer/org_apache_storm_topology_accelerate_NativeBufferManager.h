/* DO NOT EDIT THIS FILE - it is machine generated */
#include <jni.h>
/* Header for class org_apache_storm_topology_jni_DataTransfer */

#ifndef _Included_org_apache_storm_topology_accelerate_NativeBufferManager
#define _Included_org_apache_storm_topology_accelerate_NativeBufferManager
#ifdef __cplusplus
extern "C" {
#endif

/*
 * Class:     org_apache_storm_topology_accelerate_NativeBufferManager
 * Method:    shmGet
 * Signature: (IILjava/lang/String;)I
 */
JNIEXPORT jint JNICALL Java_org_apache_storm_topology_accelerate_NativeBufferManager_shmGet
  (JNIEnv *, jobject, jint, jstring);


/*
 * Class:     org_apache_storm_topology_accelerate_NativeBufferManager
 * Method:    shmClear
 * Signature: ([I)V
 */
 /*
JNIEXPORT void JNICALL Java_org_apache_storm_topology_accelerate_NativeBufferManager_shmClear
  (JNIEnv *, jobject, jintArray);*/

/*
 * Class:     org_apache_storm_topology_accelerate_NativeBufferManager
 * Method:    putIntToNativeShm
 * Signature: (I[II)Z
 */
JNIEXPORT jboolean JNICALL Java_org_apache_storm_topology_accelerate_NativeBufferManager_putIntToNativeShm
  (JNIEnv *, jobject, jint, jintArray, jint);

/*
 * Class:     org_apache_storm_topology_accelerate_NativeBufferManager
 * Method:    putLongToNativeShm
 * Signature: (I[JI)Z
 */
JNIEXPORT jboolean JNICALL Java_org_apache_storm_topology_accelerate_NativeBufferManager_putLongToNativeShm
  (JNIEnv *, jobject, jint, jlongArray, jint);


/*
 * Class:     org_apache_storm_topology_accelerate_NativeBufferManager
 * Method:    putShortToNativeShm
 * Signature: (I[SI)Z
 */
JNIEXPORT jboolean JNICALL Java_org_apache_storm_topology_accelerate_NativeBufferManager_putShortToNativeShm
  (JNIEnv *, jobject, jint, jshortArray, jint);

/*
 * Class:     org_apache_storm_topology_accelerate_NativeBufferManager
 * Method:    putByteToNativeShm
 * Signature: (I[BI)Z
 */
JNIEXPORT jboolean JNICALL Java_org_apache_storm_topology_accelerate_NativeBufferManager_putByteToNativeShm
  (JNIEnv *, jobject, jint, jbyteArray, jint);

/*
 * Class:     org_apache_storm_topology_accelerate_NativeBufferManager
 * Method:    putCharToNativeShm
 * Signature: (I[CI)Z
 */
JNIEXPORT jboolean JNICALL Java_org_apache_storm_topology_accelerate_NativeBufferManager_putCharToNativeShm
  (JNIEnv *, jobject, jint, jcharArray, jint);

/*
 * Class:     org_apache_storm_topology_accelerate_NativeBufferManager
 * Method:    putBooleanToNativeShm
 * Signature: (I[ZI)Z
 */
JNIEXPORT jboolean JNICALL Java_org_apache_storm_topology_accelerate_NativeBufferManager_putBooleanToNativeShm
  (JNIEnv *, jobject, jint, jbooleanArray, jint);

/*
 * Class:     org_apache_storm_topology_accelerate_NativeBufferManager
 * Method:    putFloatToNativeShm
 * Signature: (I[FI)Z
 */
JNIEXPORT jboolean JNICALL Java_org_apache_storm_topology_accelerate_NativeBufferManager_putFloatToNativeShm
  (JNIEnv *, jobject, jint, jfloatArray, jint);

/*
 * Class:     org_apache_storm_topology_accelerate_NativeBufferManager
 * Method:    putDoubleToNativeShm
 * Signature: (I[DI)Z
 */
JNIEXPORT jboolean JNICALL Java_org_apache_storm_topology_accelerate_NativeBufferManager_putDoubleToNativeShm
  (JNIEnv *, jobject, jint, jdoubleArray, jint);


/*
 * Class:     org_apache_storm_topology_accelerate_NativeBufferManager
 * Method:    getIntFromNativeShm
 * Signature: (I[II)V
 */
JNIEXPORT void JNICALL Java_org_apache_storm_topology_accelerate_NativeBufferManager_getIntFromNativeShm
  (JNIEnv *, jobject, jint, jintArray, jint);

/*
 * Class:     org_apache_storm_topology_accelerate_NativeBufferManager
 * Method:    getLongFromNativeShm
 * Signature: (I[JI)V
 */
JNIEXPORT void JNICALL Java_org_apache_storm_topology_accelerate_NativeBufferManager_getLongFromNativeShm
  (JNIEnv *, jobject, jint, jlongArray, jint);

/*
 * Class:     org_apache_storm_topology_accelerate_NativeBufferManager
 * Method:    getShortFromNativeShm
 * Signature: (I[SI)V
 */
JNIEXPORT void JNICALL Java_org_apache_storm_topology_accelerate_NativeBufferManager_getShortFromNativeShm
  (JNIEnv *, jobject, jint, jshortArray, jint);

/*
 * Class:     org_apache_storm_topology_accelerate_NativeBufferManager
 * Method:    getByteFromNativeShm
 * Signature: (I[BI)V
 */
JNIEXPORT void JNICALL Java_org_apache_storm_topology_accelerate_NativeBufferManager_getByteFromNativeShm
  (JNIEnv *, jobject, jint, jbyteArray, jint);

/*
 * Class:     org_apache_storm_topology_accelerate_NativeBufferManager
 * Method:    getCharacterFormNativeShm
 * Signature: (I[CI)V
 */
JNIEXPORT void JNICALL Java_org_apache_storm_topology_accelerate_NativeBufferManager_getCharFromNativeShm
  (JNIEnv *, jobject, jint, jcharArray, jint);

/*
 * Class:     org_apache_storm_topology_accelerate_NativeBufferManager
 * Method:    getBooleanFromNativeShm
 * Signature: (I[ZI)V
 */
JNIEXPORT void JNICALL Java_org_apache_storm_topology_accelerate_NativeBufferManager_getBooleanFromNativeShm
  (JNIEnv *, jobject, jint, jbooleanArray, jint);

/*
 * Class:     org_apache_storm_topology_accelerate_NativeBufferManager
 * Method:    getFloatFromNativeShm
 * Signature: (I[FI)V
 */
JNIEXPORT void JNICALL Java_org_apache_storm_topology_accelerate_NativeBufferManager_getFloatFromNativeShm
  (JNIEnv *, jobject, jint, jfloatArray, jint);

/*
 * Class:     org_apache_storm_topology_accelerate_NativeBufferManager
 * Method:    getDoubleFromNativeShm
 * Signature: (I[DI)V
 */
JNIEXPORT void JNICALL Java_org_apache_storm_topology_accelerate_NativeBufferManager_getDoubleFromNativeShm
  (JNIEnv *, jobject, jint, jdoubleArray, jint);

/*
 * Class:     org_apache_storm_topology_accelerate_NativeBufferManager
 * Method:    setInputDataReady
 * Signature: (I)V
 */
JNIEXPORT void JNICALL Java_org_apache_storm_topology_accelerate_NativeBufferManager_setInputDataReady
  (JNIEnv *, jobject, jint);

/*
 * Class:     org_apache_storm_topology_accelerate_NativeBufferManager
 * Method:    waitOutputDataReady
 * Signature: (I)V
 */
JNIEXPORT void JNICALL Java_org_apache_storm_topology_accelerate_NativeBufferManager_waitOutputDataReady
  (JNIEnv *, jobject, jint);

/*
 * Class:     org_apache_storm_topology_accelerate_NativeBufferManager
 * Method:    setOutputDataConsumed
 * Signature: (I)V
 */
JNIEXPORT void JNICALL Java_org_apache_storm_topology_accelerate_NativeBufferManager_setOutputDataConsumed
  (JNIEnv *, jobject, jint);

/*
 * Class:     org_apache_storm_topology_accelerate_NativeBufferManager
 * Method:    setInputDataEnd
 * Signature: (I)V
 */
JNIEXPORT void JNICALL Java_org_apache_storm_topology_accelerate_NativeBufferManager_setInputDataEnd
  (JNIEnv *, jobject, jint);

/*
 * Class:     org_apache_storm_topology_accelerate_NativeBufferManager
 * Method:    waitInputDataConsumed
 * Signature: (I)V
 */
JNIEXPORT void JNICALL Java_org_apache_storm_topology_accelerate_NativeBufferManager_waitInputDataConsumed
  (JNIEnv *, jobject, jint);


#ifdef __cplusplus
}
#endif
#endif
