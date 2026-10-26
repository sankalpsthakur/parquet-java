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
package org.apache.parquet.hadoop;

import static org.apache.parquet.column.Encoding.PLAIN_DICTIONARY;
import static org.apache.parquet.column.Encoding.RLE_DICTIONARY;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.ZSTD;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.assertj.core.api.Assertions.assertThat;

import com.github.luben.zstd.Zstd;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.bytes.TrackingByteBufferAllocator;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * GH-3712: ZSTD + dictionary-encoded {@code ARRAY<ARRAY<BIGINT>>} pages must record an
 * {@code uncompressed_page_size} that matches the actual decompressed payload.
 */
public class TestZstdDictionaryArrayPageSize {

  private static final int NESTED_ARRAY_VALUE_COUNT = 2898;

  private static final MessageType SCHEMA = Types.buildMessage()
      .optionalList()
      .optionalListElement()
      .optionalElement(INT64)
      .named("col")
      .named("msg");

  @TempDir
  private java.nio.file.Path tempDir;

  private TrackingByteBufferAllocator allocator;

  @BeforeEach
  public void initAllocator() {
    allocator = TrackingByteBufferAllocator.wrap(new HeapByteBufferAllocator());
  }

  @AfterEach
  public void closeAllocator() {
    allocator.close();
  }

  @ParameterizedTest
  @EnumSource(WriterVersion.class)
  public void roundTripNestedArrayPagesHaveMatchingUncompressedSize(WriterVersion version) throws Exception {
    ColumnDescriptor leaf = SCHEMA.getColumns().get(0);
    assertThat(leaf.getMaxRepetitionLevel()).isEqualTo(2);
    assertThat(leaf.getMaxDefinitionLevel()).isEqualTo(5);

    Configuration conf = new Configuration();
    GroupWriteSupport.setSchema(SCHEMA, conf);
    SimpleGroupFactory factory = new SimpleGroupFactory(SCHEMA);
    Path path = new Path(tempDir.resolve(UUID.randomUUID() + ".parquet").toUri());

    List<String> expected = new ArrayList<>();
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path)
        .withAllocator(allocator)
        .withConf(conf)
        .withCompressionCodec(ZSTD)
        .withWriterVersion(version)
        .enableDictionaryEncoding()
        .withDictionaryPageSize(1024)
        .withPageSize(2048)
        .withPageRowCountLimit(200)
        .withMinRowCountForPageSizeCheck(1)
        .build()) {
      for (int i = 0; i < NESTED_ARRAY_VALUE_COUNT; i++) {
        Group row = rowForIndex(factory, i);
        expected.add(row.toString());
        writer.write(row);
      }
    }

    try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), path)
        .withConf(conf)
        .build()) {
      int read = 0;
      for (Group group = reader.read(); group != null; group = reader.read()) {
        assertThat(group).asString().isEqualTo(expected.get(read));
        read++;
      }
      assertThat(read).isEqualTo(expected.size());
    }

    try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, conf))) {
      assertThat(reader.getRowGroups()).isNotEmpty();
      for (BlockMetaData block : reader.getFooter().getBlocks()) {
        for (ColumnChunkMetaData column : block.getColumns()) {
          if (version == PARQUET_1_0) {
            assertThat(column.getEncodings()).contains(PLAIN_DICTIONARY);
          } else {
            assertThat(column.getEncodings()).contains(RLE_DICTIONARY);
          }
          assertPageUncompressedSizesMatch(reader.f, column);
        }
      }
    }
  }

  private static Group rowForIndex(SimpleGroupFactory factory, int i) {
    Group row = factory.newGroup();
    int pattern = i % 7;
    long value = i % 8;
    switch (pattern) {
      case 0:
        return row;
      case 1:
        row.addGroup("col");
        return row;
      case 2:
        row.addGroup("col").addGroup("list");
        return row;
      case 3:
        row.addGroup("col").addGroup("list").addGroup("element");
        return row;
      case 4:
        row.addGroup("col")
            .addGroup("list")
            .addGroup("element")
            .addGroup("list")
            .append("element", value);
        return row;
      case 5: {
        Group inner = row.addGroup("col").addGroup("list").addGroup("element");
        inner.addGroup("list").append("element", value);
        inner.addGroup("list").append("element", value);
        return row;
      }
      case 6: {
        Group col = row.addGroup("col");
        col.addGroup("list").addGroup("element").addGroup("list").append("element", value);
        Group second = col.addGroup("list").addGroup("element");
        for (int j = 0; j < (i % 5) + 1; j++) {
          second.addGroup("list").append("element", value);
        }
        return row;
      }
      default:
        throw new IllegalStateException("unexpected pattern " + pattern);
    }
  }

  private static void assertPageUncompressedSizesMatch(SeekableInputStream in, ColumnChunkMetaData column)
      throws IOException {
    long start = column.getStartingPos();
    long end = start + column.getTotalSize();
    in.seek(start);
    while (in.getPos() < end) {
      PageHeader header = Util.readPageHeader(in);
      switch (header.type) {
        case DICTIONARY_PAGE:
        case DATA_PAGE:
          byte[] compressed = readFully(in, header.getCompressed_page_size());
          assertZstdPayloadMatchesHeader(compressed, header.getUncompressed_page_size());
          break;
        case DATA_PAGE_V2:
          assertV2PayloadMatchesHeader(in, header);
          break;
        default:
          throw new IOException("Unexpected page type " + header.type);
      }
    }
  }

  private static void assertV2PayloadMatchesHeader(SeekableInputStream in, PageHeader header) throws IOException {
    DataPageHeaderV2 v2 = header.getData_page_header_v2();
    int rl = v2.getRepetition_levels_byte_length();
    int dl = v2.getDefinition_levels_byte_length();
    readFully(in, rl);
    readFully(in, dl);
    int compressedData = header.getCompressed_page_size() - rl - dl;
    byte[] compressed = readFully(in, compressedData);
    int actualData;
    if (v2.isIs_compressed() && compressedData > 0) {
      int declaredData = header.getUncompressed_page_size() - rl - dl;
      actualData = zstdDecompressedSize(compressed, declaredData);
    } else {
      actualData = compressed.length;
    }
    assertThat(rl + dl + actualData).isEqualTo(header.getUncompressed_page_size());
  }

  private static void assertZstdPayloadMatchesHeader(byte[] compressed, int declaredUncompressed) {
    assertThat(zstdDecompressedSize(compressed, declaredUncompressed)).isEqualTo(declaredUncompressed);
  }

  private static int zstdDecompressedSize(byte[] compressed, int minDstSize) {
    long frameSize = Zstd.decompressedSize(compressed);
    int dstSize = minDstSize + 1024;
    if (frameSize > dstSize) {
      dstSize = Math.toIntExact(frameSize);
    }
    byte[] dst = new byte[Math.max(dstSize, 1)];
    return Math.toIntExact(Zstd.decompress(dst, compressed));
  }

  private static byte[] readFully(SeekableInputStream in, int len) throws IOException {
    byte[] bytes = new byte[len];
    if (len > 0) {
      in.readFully(bytes);
    }
    return bytes;
  }
}
