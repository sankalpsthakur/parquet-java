/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.column.values.rle;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.bytes.TrackingByteBufferAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * GH-3712: the RLE length prefix and page uncompressed_page_size are taken from
 * {@code BytesInput.size()}, while the payload is serialized from slab contents.
 * Those two must match, including the padded final bit-packed group of a nested
 * ARRAY repetition-level stream (bitWidth=2, 2898 values).
 */
public class TestRunLengthBitPackingHybridEncoderSize {

  private TrackingByteBufferAllocator allocator;

  @BeforeEach
  public void initAllocator() {
    allocator = TrackingByteBufferAllocator.wrap(new HeapByteBufferAllocator());
  }

  @AfterEach
  public void closeAllocator() {
    allocator.close();
  }

  @Test
  public void encodedSizeMatchesSerializedBytesForNestedArrayLevels() throws Exception {
    // maxRepetitionLevel=2 → bitWidth 2; 2898 is the valueCount from GH-3712
    int bitWidth = 2;
    int valueCount = 2898;
    try (RunLengthBitPackingHybridEncoder encoder =
        new RunLengthBitPackingHybridEncoder(bitWidth, 64, 1024, allocator)) {
      for (int i = 0; i < valueCount; i++) {
        // Mix repeats (RLE runs) with changing values (bit-packed runs, including a padded tail)
        int level;
        if (i % 17 == 0) {
          level = 0;
        } else if (i % 5 == 0) {
          level = 1;
        } else {
          level = i % 3;
        }
        encoder.writeInt(level);
      }

      BytesInput encoded = encoder.toBytes();
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      encoded.writeAllTo(out);
      byte[] serialized = out.toByteArray();
      assertThat(serialized).hasSize((int) encoded.size());

      RunLengthBitPackingHybridDecoder decoder =
          new RunLengthBitPackingHybridDecoder(bitWidth, encoded.toInputStream());
      for (int i = 0; i < valueCount; i++) {
        int expected;
        if (i % 17 == 0) {
          expected = 0;
        } else if (i % 5 == 0) {
          expected = 1;
        } else {
          expected = i % 3;
        }
        assertThat(decoder.readInt()).isEqualTo(expected);
      }
    }
  }
}
