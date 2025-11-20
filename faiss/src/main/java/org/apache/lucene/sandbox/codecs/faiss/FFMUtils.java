/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.sandbox.codecs.faiss;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.Optional;

/**
 * Compatibility layer for Java 21 and Java 22+ Foreign Function & Memory API differences.
 *
 * @lucene.experimental
 */
final class FFMUtils {
  private static final int JAVA_VERSION = Runtime.version().feature();
  private static final boolean IS_JAVA_21 = JAVA_VERSION == 21;

  private static final MethodHandle SYMBOL_LOOKUP_FIND;
  private static final MethodHandle MEMORY_SEGMENT_GET_UTF8_STRING;
  private static final MethodHandle MEMORY_SEGMENT_GET_STRING;
  private static final MethodHandle ARENA_ALLOCATE;
  private static final MethodHandle ARENA_ALLOCATE_FROM_STRING;
  private static final MethodHandle ARENA_ALLOCATE_FROM_FLOAT;
  private static final MethodHandle ARENA_ALLOCATE_FROM_LONG;

  static {
    try {
      MethodHandles.Lookup lookup = MethodHandles.lookup();

      SYMBOL_LOOKUP_FIND =
          lookup.findVirtual(
              SymbolLookup.class,
              "find",
              MethodType.methodType(Optional.class, String.class));

      if (IS_JAVA_21) {
        MEMORY_SEGMENT_GET_UTF8_STRING =
            lookup.findVirtual(
                MemorySegment.class, "getUtf8String", MethodType.methodType(String.class, long.class));
        MEMORY_SEGMENT_GET_STRING = null;

        ARENA_ALLOCATE =
            lookup.findVirtual(
                Arena.class, "allocate", MethodType.methodType(MemorySegment.class, long.class));
        ARENA_ALLOCATE_FROM_STRING = null;
        ARENA_ALLOCATE_FROM_FLOAT = null;
        ARENA_ALLOCATE_FROM_LONG = null;
      } else {
        MEMORY_SEGMENT_GET_UTF8_STRING = null;
        MEMORY_SEGMENT_GET_STRING =
            lookup.findVirtual(
                MemorySegment.class, "getString", MethodType.methodType(String.class, long.class));

        ARENA_ALLOCATE = null;
        ARENA_ALLOCATE_FROM_STRING =
            lookup.findVirtual(
                Arena.class,
                "allocateFrom",
                MethodType.methodType(MemorySegment.class, String.class));
        ARENA_ALLOCATE_FROM_FLOAT =
            lookup.findVirtual(
                Arena.class,
                "allocateFrom",
                MethodType.methodType(MemorySegment.class, ValueLayout.OfFloat.class, float[].class));
        ARENA_ALLOCATE_FROM_LONG =
            lookup.findVirtual(
                Arena.class,
                "allocateFrom",
                MethodType.methodType(MemorySegment.class, ValueLayout.OfLong.class, long[].class));
      }
    } catch (NoSuchMethodException | IllegalAccessException e) {
        throw new RuntimeException("Failed to initialize FFM utils", e);
    }
  }

  private FFMUtils() {}

  /**
   * Find a symbol in SymbolLookup.
   * Both Java 21 and Java 22 use find() which returns Optional<MemorySegment>.
   */
  static MemorySegment findSymbol(SymbolLookup lookup, String name) {
    try {
      @SuppressWarnings("unchecked")
      Optional<MemorySegment> optional = (Optional<MemorySegment>) SYMBOL_LOOKUP_FIND.invokeExact(lookup, name);
      return optional.orElseThrow(() -> new UnsatisfiedLinkError("Symbol not found: " + name));
    } catch (Throwable e) {
      if (e instanceof UnsatisfiedLinkError) {
        throw (UnsatisfiedLinkError) e;
      }
      throw new RuntimeException("Failed to find symbol: " + name, e);
    }
  }

  /**
   * Get a UTF-8 string from MemorySegment.
   */
  static String getString(MemorySegment segment, long offset) {
    try {
      if (IS_JAVA_21) {
        return (String) MEMORY_SEGMENT_GET_UTF8_STRING.invokeExact(segment, offset);
      } else {
        return (String) MEMORY_SEGMENT_GET_STRING.invokeExact(segment, offset);
      }
    } catch (Throwable e) {
      throw new RuntimeException("Failed to get string from MemorySegment", e);
    }
  }

  /**
   * Allocate a MemorySegment from a string.
   */
  static MemorySegment allocateFrom(Arena arena, String str) {
    try {
      if (IS_JAVA_21) {
        byte[] bytes = str.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        MemorySegment segment = (MemorySegment) ARENA_ALLOCATE.invokeExact(arena, (long) (bytes.length + 1));
        MemorySegment.copy(
            MemorySegment.ofArray(bytes), ValueLayout.JAVA_BYTE, 0, segment, ValueLayout.JAVA_BYTE, 0, (long) bytes.length);
        segment.set(ValueLayout.JAVA_BYTE, bytes.length, (byte) 0);
        return segment;
      } else {
        return (MemorySegment) ARENA_ALLOCATE_FROM_STRING.invokeExact(arena, str);
      }
    } catch (Throwable e) {
      throw new RuntimeException("Failed to allocate string in native memory", e);
    }
  }

  /**
   * Allocate a MemorySegment from a float array.
   */
  static MemorySegment allocateFrom(Arena arena, ValueLayout.OfFloat layout, float[] array) {
    try {
      if (IS_JAVA_21) {
        long size = (long) array.length * layout.byteSize();
        MemorySegment segment = (MemorySegment) ARENA_ALLOCATE.invokeExact(arena, size);
        MemorySegment.copy(
            MemorySegment.ofArray(array), layout, 0, segment, layout, 0, (long) array.length);
        return segment;
      } else {
        return (MemorySegment) ARENA_ALLOCATE_FROM_FLOAT.invokeExact(arena, layout, array);
      }
    } catch (Throwable e) {
      throw new RuntimeException("Failed to allocate float array in native memory", e);
    }
  }

  /**
   * Allocate a MemorySegment from a long array.
   */
  static MemorySegment allocateFrom(Arena arena, ValueLayout.OfLong layout, long[] array) {
    try {
      if (IS_JAVA_21) {
        long size = (long) array.length * layout.byteSize();
        MemorySegment segment = (MemorySegment) ARENA_ALLOCATE.invokeExact(arena, size);
        MemorySegment.copy(
            MemorySegment.ofArray(array), layout, 0, segment, layout, 0, (long) array.length);
        return segment;
      } else {
        return (MemorySegment) ARENA_ALLOCATE_FROM_LONG.invokeExact(arena, layout, array);
      }
    } catch (Throwable e) {
      throw new RuntimeException("Failed to allocate long array in native memory", e);
    }
  }
}

