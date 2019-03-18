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

package org.apache.hadoop.io.compress.lzfse;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.LzfseCodec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.NativeCodeLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LzfseDecompressor implements Decompressor {

  private static final Logger LOG = LoggerFactory.getLogger(LzfseDecompressor.class.getName());

  private static final int DEFAULT_INTERNAL_BUFFER_SIZE = 4*1024;

  private byte[] userBuffer = null;
  private int userBufferOff = 0;
  private int userBufferLen = 0;

  private Buffer compressedBuffer = null;
  private Buffer decompressedBuffer = null;
  private int internalBufferSize = 0;

  private boolean finish = false;

  private static boolean nativeLzfseLoaded = false;

  static {
    if (NativeCodeLoader.isNativeCodeLoaded()) {
      try {
        initIDs();
        nativeLzfseLoaded = true;
      } catch (Throwable t) {
        LOG.warn("Error loading lzfse native libraries: " + t);
      }
    } else {
      LOG.error("Cannot load " + LzfseDecompressor.class.getName() + " without native hadoop library!");
    }
  }

  private native static void initIDs();

  public static boolean isNativeCodeLoaded() {
    return nativeLzfseLoaded;
  }

  public LzfseDecompressor() {
    this(DEFAULT_INTERNAL_BUFFER_SIZE);
  }

  public LzfseDecompressor(int internalBufferSize) {
    this.internalBufferSize = internalBufferSize;
    // both are clear, in the write mode
    compressedBuffer = ByteBuffer.allocateDirect(internalBufferSize);
    decompressedBuffer = ByteBuffer.allocateDirect(internalBufferSize);
    // enter read mode
    decompressedBuffer.limit(0);
  }

  @Override
  public int decompress(byte[] b, int off, int len) {
    if (b == null) {
      throw new NullPointerException();
    }
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new ArrayIndexOutOfBoundsException();
    }

    if (needsInput()) {
      return 0;
    }

    // buffer and decompress data if nothing to be returned
    if (!decompressedBuffer.hasRemaining()) {

      // buffer user data
      compressedBuffer.clear();
      int buffered = Math.min(userBufferLen, compressedBuffer.remaining());
      ((ByteBuffer) compressedBuffer).put(userBuffer, userBufferOff, buffered);
      userBufferLen -= buffered;
      userBufferOff += buffered;

      // compress
      decompressedBuffer.clear(); // write mode
      int n = decompressBytesDirect(buffered);
      decompressedBuffer.limit(n); // read mode

      // compressed buffer state is meaningless, will be cleared when needed
    }

    // return decompressed data
    int copied = Math.min(len, decompressedBuffer.remaining());
    ((ByteBuffer) decompressedBuffer).get(b, off, copied);
    return copied;
  }

  private native int decompressBytesDirect(int size);

  @Override
  public void end() {

  }

  @Override
  public boolean finished() {
    // only if all buffers are empty
    return userBufferLen == 0 && !decompressedBuffer.hasRemaining();
  }

  @Override
  public int getRemaining() {
    return userBufferLen;
  }

  @Override
  public boolean needsDictionary() {
    return false;
  }

  @Override
  public boolean needsInput() {
    // only if all buffers are empty
    return userBufferLen == 0 && !decompressedBuffer.hasRemaining();
  }

  @Override
  public void reset() {
    userBuffer = null;
    userBufferOff = 0;
    userBufferLen = 0;

    // clear, enter write mode
    compressedBuffer.clear();
    // clear, enter read mode
    decompressedBuffer.position(0).limit(0);
  }

  @Override
  public void setDictionary(byte[] b, int off, int len) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setInput(byte[] b, int off, int len) {
    if (b == null) {
      throw new NullPointerException();
    }
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new ArrayIndexOutOfBoundsException();
    }
    if (userBufferLen > 0) {
      throw new IllegalStateException("Input is not needed!");
    }

    // save user data
    userBuffer = b;
    userBufferOff = off;
    userBufferLen = len;
  }
}
