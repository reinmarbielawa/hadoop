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

import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.LzfseCodec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.NativeCodeLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LzfseCompressor implements Compressor {

  private static final Logger LOG = LoggerFactory.getLogger(LzfseCompressor.class.getName());

  private static final int DEFAULT_INTERNAL_BUFFER_SIZE = 4*1024;

  private byte[] userBuffer = null;
  private int userBufferOff = 0;
  private int userBufferLen = 0;

  private Buffer uncompressedBuffer = null;
  private Buffer compressedBuffer = null;
  private int internalBufferSize = 0;

  private boolean finish = false;

  private long bytesRead = 0;
  private long bytesWritten = 0;

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
      LOG.error("Cannot load " + LzfseCompressor.class.getName() + " without native hadoop library!");
    }
  }

  private native static void initIDs();

  public static boolean isNativeCodeLoaded() {
    return nativeLzfseLoaded;
  }

  public LzfseCompressor() {
    this(DEFAULT_INTERNAL_BUFFER_SIZE);
  }

  public LzfseCompressor(int internalBufferSize) {
    this.internalBufferSize = internalBufferSize;
    // both are clear, in the write mode
    uncompressedBuffer = ByteBuffer.allocateDirect(internalBufferSize);
    compressedBuffer = ByteBuffer.allocateDirect(internalBufferSize);
    // enter read mode
    compressedBuffer.limit(0);
  }

  @Override
  public int compress(byte[] b, int off, int len) {
    if (b == null) {
      throw new NullPointerException();
    }
    if (off < 0 || len < 0 || off > b.length - len) {
      throw new ArrayIndexOutOfBoundsException();
    }

    // buffer and compress data if nothing to be returned
    if (!compressedBuffer.hasRemaining()) {

      // buffer user data
      uncompressedBuffer.clear();
      int buffered = Math.min(userBufferLen, uncompressedBuffer.remaining());
      ((ByteBuffer) uncompressedBuffer).put(userBuffer, userBufferOff, buffered);
      userBufferLen -= buffered;
      userBufferOff += buffered;
      bytesRead += buffered;

      // compress
      compressedBuffer.clear(); // write mode
      int n = compressBytesDirect(buffered);
      compressedBuffer.limit(n); // read mode

      // uncompressed buffer state is meaningless, will be cleared when needed
    }

    // return compressed data
    int copied = Math.min(len, compressedBuffer.remaining());
    ((ByteBuffer) compressedBuffer).get(b, off, copied);
    bytesWritten += copied;
    return copied;
  }

  private native int compressBytesDirect(int size);

  @Override
  public void end() {
  }

  @Override
  public void finish() {
    finish = true;
  }

  @Override
  public boolean finished() {
    return finish && userBufferLen == 0 && !compressedBuffer.hasRemaining();
  }

  @Override
  public long getBytesRead() {
    return bytesRead;
  }

  @Override
  public long getBytesWritten() {
    return bytesWritten;
  }

  @Override
  public boolean needsInput() {
    // only if all buffers are empty
    return userBufferLen == 0 && !compressedBuffer.hasRemaining();
  }

  @Override
  public void reinit(Configuration conf) {
    reset();
  }

  @Override
  public void reset() {
    userBuffer = null;
    userBufferOff = 0;
    userBufferLen = 0;

    // clear, enter write mode
    uncompressedBuffer.clear();
    // clear, enter read mode
    compressedBuffer.position(0).limit(0);

    finish = false;

    bytesRead = 0;
    bytesWritten = 0;
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

  public native static String getLibraryName();
}
