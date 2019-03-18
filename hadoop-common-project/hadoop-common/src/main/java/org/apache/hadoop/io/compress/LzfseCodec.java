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

package org.apache.hadoop.io.compress;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.lzfse.LzfseCompressor;
import org.apache.hadoop.io.compress.lzfse.LzfseDecompressor;
import org.apache.hadoop.util.NativeCodeLoader;

public class LzfseCodec implements Configurable, CompressionCodec {

  private Configuration conf = null;

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  public static void checkNativeCodeLoaded() {
    if (!NativeCodeLoader.buildSupportsLzfse()) {
      throw new RuntimeException("native lzfse library not available: " +
          "this version of libhadoop was built without " +
          "lzfse support.");
    }
    if (!NativeCodeLoader.isNativeCodeLoaded()) {
      throw new RuntimeException("Failed to load libhadoop.");
    }
    if (!LzfseCompressor.isNativeCodeLoaded()) {
      throw new RuntimeException("native lzfse library not available: " +
          "LzfseCompressor has not been loaded.");
    }
    if (!LzfseDecompressor.isNativeCodeLoaded()) {
      throw new RuntimeException("native lzfse library not available: " +
          "LzfseDecompressor has not been loaded.");
    }
  }

  public static boolean isNativeCodeLoaded() {
    return LzfseCompressor.isNativeCodeLoaded() &&
        LzfseDecompressor.isNativeCodeLoaded();
  }

  public static String getLibraryName() {
    return LzfseCompressor.getLibraryName();
  }

  @Override
  public Compressor createCompressor() {
    checkNativeCodeLoaded();
    return new LzfseCompressor();
  }

  @Override
  public Decompressor createDecompressor() {
    checkNativeCodeLoaded();
    return new LzfseDecompressor();
  }

  @Override
  public CompressionInputStream createInputStream(InputStream in) throws IOException {
    return CompressionCodec.Util.createInputStreamWithCodecPool(this, conf, in);
  }

  @Override
  public CompressionInputStream createInputStream(InputStream in, Decompressor decompressor) throws IOException {
    checkNativeCodeLoaded();
    return new DecompressorStream(in, decompressor);
  }

  @Override
  public CompressionOutputStream createOutputStream(OutputStream out) throws IOException {
    return CompressionCodec.Util.createOutputStreamWithCodecPool(this, conf, out);
  }

  @Override
  public CompressionOutputStream createOutputStream(OutputStream out, Compressor compressor) throws IOException {
    checkNativeCodeLoaded();
    return new CompressorStream(out, compressor);
  }

  @Override
  public Class<? extends Compressor> getCompressorType() {
    checkNativeCodeLoaded();
    return LzfseCompressor.class;
  }

  @Override
  public Class<? extends Decompressor> getDecompressorType() {
    checkNativeCodeLoaded();
    return LzfseDecompressor.class;
  }

  @Override
  public String getDefaultExtension() {
    return ".lzfse";
  }
}
