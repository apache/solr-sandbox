# FAISS Module for Solr

FAISS integration for Apache Solr. Backported from Lucene 11.0 to work with Lucene 10.3.1.

## Requirements

- Java 21+
- FAISS native library (`libfaiss_c.so` version 1.11.0)
- Solr 9.2.0+

## Building

```bash
./gradlew :faiss:jar
```

JAR is created at `faiss/build/libs/solr-faiss.jar`.

## Installation

Copy the JAR to Solr's lib directory:

```bash
cp faiss/build/libs/solr-faiss.jar $SOLR_HOME/server/solr-webapp/webapp/WEB-INF/lib/
```

Set `LD_LIBRARY_PATH` to include the FAISS library directory.

## Configuration

### solrconfig.xml

```xml
<codecFactory name="FaissCodecFactory" class="org.apache.solr.faiss.FaissCodecFactory">
  <str name="faissDescription">IDMap,HNSW16</str>
  <str name="faissIndexParams">efConstruction=100</str>
</codecFactory>
```

### schema.xml

```xml
<fieldType name="faiss_vector" 
           class="solr.DenseVectorField" 
           vectorDimension="128" 
           knnAlgorithm="faiss" 
           similarityFunction="dot_product"/>

<field name="vector_field" type="faiss_vector" indexed="true" stored="true"/>
```

Note: FAISS supports `dot_product` and `euclidean`, but not `cosine`.

## Testing

Tests require the Solr branch_10_0 submodule. Clone with `--recursive` or run:

```bash
git submodule update --init --recursive
./gradlew :faiss:test
```

Without the submodule, tests use published Solr 9.2.0 artifacts and may fail due to Lucene version mismatch.

## Git Submodule

This module uses a git submodule pointing to Solr branch_10_0 for tests. Only a reference (~200 bytes) is stored, not the actual Solr code.

To clone with submodule:
```bash
git clone <repo-url> --recursive
```

## Troubleshooting

- **ClassNotFoundException** - Ensure JAR is in Solr's classpath
- **UnsatisfiedLinkError** - Set `LD_LIBRARY_PATH` to include `libfaiss_c.so`
- **LinkageError** - Requires exact FAISS version 1.11.0
- **Tests fail** - Initialize submodule: `git submodule update --init --recursive`
