/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


#include "org_apache_hadoop_io_compress_lzfse.h"
#include "org_apache_hadoop_io_compress_lzfse_LzfseCompressor.h"

static jfieldID LzfseCompressor_uncompressedBuffer;
static jfieldID LzfseCompressor_compressedBuffer;
static jfieldID LzfseCompressor_internalBufferSize;

static LZFSE_API size_t (*dlsym_lzfse_encode_buffer)(uint8_t *, size_t, const uint8_t *, size_t, void *);

JNIEXPORT void JNICALL Java_org_apache_hadoop_io_compress_lzfse_LzfseCompressor_initIDs
(JNIEnv *env, jclass clazz) {
  // Load liblzfse.so
  void *liblzfse = dlopen(HADOOP_LZFSE_LIBRARY, RTLD_LAZY | RTLD_GLOBAL);
  if (!liblzfse) {
    char msg[1000];
    snprintf(msg, 1000, "%s (%s)!", "Cannot load " HADOOP_LZFSE_LIBRARY, dlerror());
    THROW(env, "java/lang/UnsatisfiedLinkError", msg);
    return;
  }

  // Locate the required symbols
  dlerror();    // Clear any existing error
  LOAD_DYNAMIC_SYMBOL(dlsym_lzfse_encode_buffer, env, liblzfse, "lzfse_encode_buffer");

  LzfseCompressor_uncompressedBuffer = (*env)->GetFieldID(env, clazz, "uncompressedBuffer", "Ljava/nio/Buffer;");
  LzfseCompressor_compressedBuffer = (*env)->GetFieldID(env, clazz, "compressedBuffer", "Ljava/nio/Buffer;");
  LzfseCompressor_internalBufferSize = (*env)->GetFieldID(env, clazz, "internalBufferSize", "I");
}

JNIEXPORT jsize JNICALL Java_org_apache_hadoop_io_compress_lzfse_LzfseCompressor_compressBytesDirect
(JNIEnv *env, jobject thisj, jint size) {

  jobject uncompressed_buffer = (*env)->GetObjectField(env, thisj, LzfseCompressor_uncompressedBuffer);
  uint8_t *src_buffer = (uint8_t *)(*env)->GetDirectBufferAddress(env, uncompressed_buffer);
  size_t src_size = (size_t) size;

  jobject compressed_buffer = (*env)->GetObjectField(env, thisj, LzfseCompressor_compressedBuffer);
  uint8_t *dst_buffer = (uint8_t *)(*env)->GetDirectBufferAddress(env, compressed_buffer);
  size_t dst_size = (size_t)(*env)->GetIntField(env, thisj, LzfseCompressor_internalBufferSize);

  jsize encoded = (jsize) dlsym_lzfse_encode_buffer(dst_buffer, dst_size, src_buffer, src_size, NULL);
  if (encoded == 0 && src_size != 0){
    THROW(env, "java/lang/InternalError", "lzfse_encode_buffer failed");
  }

  return encoded;
}

JNIEXPORT jstring JNICALL Java_org_apache_hadoop_io_compress_lzfse_LzfseCompressor_getLibraryName
(JNIEnv *env, jclass clazz) {
  if (dlsym_lzfse_encode_buffer) {
    Dl_info dl_info;
    if(dladdr(dlsym_lzfse_encode_buffer, &dl_info)) {
      return (*env)->NewStringUTF(env, dl_info.dli_fname);
    }
  }
  return (*env)->NewStringUTF(env, HADOOP_LZFSE_LIBRARY);
}
