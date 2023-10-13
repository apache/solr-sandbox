# Solr Encryption: Getting Started

**A Java-level encryption-at-rest solution for Apache Solr.**

## Overview

This solution provides the encryption of the Lucene index files at the Java level.
It encrypts all (or some) the files in a given index with a provided encryption key.
It stores the id of the encryption key in the commit metadata (and obviously the
key secret is never stored). It is possible to define a different key per Solr Core.
This module also provides an EncryptionRequestHandler so that a client can trigger
the (re)encryption of a Solr Core index. The (re)encryption is done concurrently
while the Solr Core can continue to serve update and query requests.

In addition, the Solr update logs are also encrypted when the Solr Core index is
encrypted. When the active encryption key changes for the Solr Core, the
re-encryption of the update logs is done synchronously when an old log file is
opened for addition. This re-encryption is nearly as fast as a file copy.

Comparing with an OS-level encryption:

- OS-level encryption [1][2] is more performant and more adapted to let Lucene
leverage the OS memory cache. It can manage encryption at block or filesystem
level in the OS. This makes it possible to encrypt with different keys per-directory,
making multi-tenant use-cases possible.
If you can use OS-level encryption, prefer it and skip this Java-level encryption.

- Java-level encryption can be used when the OS-level encryption management is
not possible (e.g. host machine managed by a cloud provider). It has an impact
on performance: expect -20% on most queries, -60% on multi-term queries.

[1] https://wiki.archlinux.org/title/Fscrypt

[2] https://www.kernel.org/doc/html/latest/filesystems/fscrypt.html

## Installing and Configuring the Encryption Plug-In

1. Configure the sharedLib directory in solr.xml (e.g. sharedLIb=lib) and place
the Encryption plug-in jar file into the specified folder.

**solr.xml**

```xml
<solr>

    <str name="sharedLib">${solr.sharedLib:}</str>

</solr>
```

2. Configure the Encryption classes in solrconfig.xml.

**solrconfig.xml**

```xml
<config>

    <directoryFactory name="DirectoryFactory"
                      class="org.apache.solr.encryption.EncryptionDirectoryFactory">
        <str name="keySupplierFactory">com.yourApp.YourKeySupplier$Factory</str>
        <str name="encrypterFactory">org.apache.solr.encryption.crypto.CipherAesCtrEncrypter$Factory</str>
    </directoryFactory>

    <updateHandler class="org.apache.solr.encryption.EncryptionUpdateHandler">
        <updateLog class="org.apache.solr.encryption.EncryptionUpdateLog">
            <str name="dir">${solr.ulog.dir:}</str>
        </updateLog>
    </updateHandler>

    <requestHandler name="/admin/encrypt" class="org.apache.solr.encryption.EncryptionRequestHandler"/>

    <mergePolicyFactory class="org.apache.solr.encryption.EncryptionMergePolicyFactory">
        <str name="wrapped.prefix">delegate</str>
        <str name="delegate.class">org.apache.solr.index.TieredMergePolicyFactory</str>
    </mergePolicyFactory>

</config>
```

`EncryptionDirectoryFactory` is the DirectoryFactory that encrypts/decrypts all (or some) the index files.

`keySupplierFactory` is a required parameter to specify your implementation of
`org.apache.solr.encryption.KeySupplier.Factory`. This class is used to get your `KeySupplier`.

`encrypterFactory` is an optional parameter to specify the `org.apache.solr.encryption.crypto.AesCtrEncrypterFactory`
to use. By default `CipherAesCtrEncrypter$Factory` is used. You can change to `LightAesCtrEncrypter$Factory` for a
more lightweight and efficient implementation (+10% perf), but it calls an internal com.sun.crypto.provider.AESCrypt()
constructor which logs a JDK warning (Illegal reflective access).

`EncryptionUpdateHandler` replaces the standard `DirectUpdateHandler2` (which it extends) to store persistently the
encryption key id in the commit metadata. It supports all the configuration parameters of `DirectUpdateHandler2`.

`EncryptionUpdateLog` replaces the standard `UpdateLog` (which it extends) to support the encryption of the update
logs.

`EncryptionRequestHandler` receives (re)encryption requests. See its dedicated section below for its usage.

`EncryptionMergePolicyFactory` is a wrapper above a delegate MergePolicyFactory (e.g. the standard
`TieredMergePolicyFactory`) to ensure all index segments are re-written (re-encrypted).

## Calling EncryptionRequestHandler

Once Solr is set up, it is ready to encrypt. To set the encryption key id to use, the Solr client
calls the `EncryptionRequestHandler` at `/admin/encrypt`.

`EncryptionRequestHandler` handles an encryption request for a specific Solr core.

The caller provides the mandatory `encryptionKeyId` request parameter to define the encryption
key id to use to encrypt the index files. To decrypt the index to cleartext, the special parameter
value `no_key_id` must be provided.

The encryption processing is asynchronous. The request returns immediately with two response parameters.
- `encryptionState` parameter with value either `pending`, `complete`, or `busy`.
- `status` parameter with values either `success` or `failure`.

The expected usage of this handler is to first send an encryption request with a key id, and to receive
a response with `status`=`success` and `encryptionState`=`pending`. If the caller needs to know when the
encryption is complete, it can (optionally) repeatedly send the same encryption request with the same key id,
until it receives a response with `status`=`success` and `encryptionState`=`complete`.

If the handler returns a response with `encryptionState`=`busy`, it means that another encryption for a
different key id is ongoing on the same Solr core. It cannot start a new encryption until it finishes.

If the handler returns a response with `status`=`failure`, it means the request did not succeed and should be
retried by the caller (there should be error logs).

## Encryption Algorithm

This encryption module implements AES-CTR.

AES-CTR compared to AES-XTS:
Lucene produces read-only files per index segment. Since we have a new random IV per file, we don't repeat
the same AES encrypted blocks. So we are in a safe write-once case where AES-XTS and AES-CTR have the same
strength [1][2]. CTR was chosen because it is simpler.

[1] https://crypto.stackexchange.com/questions/64556/aes-xts-vs-aes-ctr-for-write-once-storage

[2] https://crypto.stackexchange.com/questions/14628/why-do-we-use-xts-over-ctr-for-disk-encryption

## Performance Notes

The performance benchmark was run in LUCENE-9379. Here is the summary:

- An OS-level encryption is better and faster.
- If really it’s not possible, expect an average of -20% perf impact on most queries, -60% on multi-term queries.
- You can use the `LightAesCtrEncrypter$Factory` to get +10% perf. This is a simple config change. See the
solrconfig.xml configuration section above.
- You can make the Lucene Codec store its FST on heap and expect +15% perf, at the price of more Java heap usage.
This requires a code change. See `org.apache.lucene.util.fst.FSTStore` implementations and usage in
`org.apache.lucene.codecs.lucene90.blocktree.FieldReader`.

## Encryption tools

The `org.apache.solr.encryption.crypto` package contains utility classes to stream encryption/decryption with the
`AES/CTR/NoPadding` transformation.
`CharStreamEncrypter` can encrypt a character stream to a base 64 encoding compatible with JSON, with a small
buffer.