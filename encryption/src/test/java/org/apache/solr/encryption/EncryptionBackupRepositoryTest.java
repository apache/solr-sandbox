package org.apache.solr.encryption;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.*;
import org.apache.solr.cloud.api.collections.AbstractBackupRepositoryTest;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.core.backup.repository.BackupRepository;
import org.apache.solr.core.backup.repository.BackupRepositoryFactory;
import org.apache.solr.core.backup.repository.LocalFileSystemRepository;
import org.apache.solr.encryption.crypto.AesCtrEncrypterFactory;
import org.apache.solr.encryption.crypto.CipherAesCtrEncrypter;
import org.apache.solr.encryption.crypto.LightAesCtrEncrypter;
import org.apache.solr.schema.FieldType;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.solr.core.backup.repository.DelegatingBackupRepository.PARAM_DELEGATE_REPOSITORY_NAME;
import static org.apache.solr.encryption.EncryptionDirectory.ENCRYPTION_MAGIC;
import static org.apache.solr.encryption.EncryptionUtil.COMMIT_ACTIVE_KEY;

/**
 * Tests {@link EncryptionBackupRepository}.
 */
public class EncryptionBackupRepositoryTest extends AbstractBackupRepositoryTest {

    private static URI baseUri;

    @BeforeClass
    public static void setupBaseDir() {
        baseUri = createTempDir().toUri();
    }

    @Override
    protected Class<? extends BackupRepository> getRepositoryClass() {
        return EncryptionBackupRepository.class;
    }

    @Override
    protected BackupRepository getRepository() {
        TestEncryptionBackupRepository repo = new TestEncryptionBackupRepository();
        repo.init(getBaseBackupRepositoryConfiguration());
        BackupRepository delegate = new LocalFileSystemRepository();
        delegate.init(getBaseBackupRepositoryConfiguration());
        repo.setDelegate(delegate);
        return repo;
    }

    @Override
    protected URI getBaseUri() throws URISyntaxException {
        return baseUri;
    }

    /**
     * Ignore this inherited test. Instead, test how encrypted files are checked and copied,
     * as this relies on the same mechanism.
     */
    @Ignore
    @Override
    public void testCanDisableChecksumVerification() {
    }

    /**
     * Tests a backup of an encrypted file.
     * The checksum is verified in the decrypted form of the file, otherwise it fails.
     * The backup copy is kept in its encrypted form, and not decrypted by the {@link EncryptionDirectory}.
     */
    @Test
    public void testEncryptedBackup() throws Exception {
        // Given two BackupRepository plugins:
        // - A LocalFileSystemRepository plugin.
        // - An EncryptionBackupRepository that delegates to the previous one.
        String localRepoName = "localRepo";
        String encryptionRepoName = "encryptionRepo";
        NamedList<Object> encryptionRepoArgs = new NamedList<>();
        encryptionRepoArgs.add(PARAM_DELEGATE_REPOSITORY_NAME, localRepoName);
        PluginInfo[] plugins =
                new PluginInfo[]{
                        getPluginInfo(localRepoName, LocalFileSystemRepository.class, false, new NamedList<>()),
                        getPluginInfo(encryptionRepoName, EncryptionBackupRepository.class, true, encryptionRepoArgs),
                };
        Collections.shuffle(Arrays.asList(plugins), random());

        // Given an encrypted file on the local disk.
        Path sourcePath = createTempDir().toAbsolutePath();
        AesCtrEncrypterFactory encrypterFactory = random().nextBoolean() ? CipherAesCtrEncrypter.FACTORY : LightAesCtrEncrypter.FACTORY;
        KeySupplier keySupplier = new TestingKeySupplier.Factory().create();
        try (Directory fsSourceDir = FSDirectory.open(sourcePath);
             Directory encSourceDir = new TestEncryptionDirectory(fsSourceDir, encrypterFactory, keySupplier);
             Directory destinationDir = FSDirectory.open(createTempDir().toAbsolutePath())) {
            String fileName = "source-file";
            String content = "content";
            try (IndexOutput out = encSourceDir.createOutput(fileName, IOContext.DEFAULT)) {
                byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
                out.writeBytes(bytes, bytes.length);
                CodecUtil.writeFooter(out);
            }

            BackupRepositoryFactory repoFactory = new BackupRepositoryFactory(plugins);
            try (SolrResourceLoader resourceLoader = new SolrResourceLoader(sourcePath)) {

                // When we copy the encrypted file with the LocalFileSystemRepository,
                // then it fails because the encrypted checksum is invalid.
                String destinationFolder = "destination-folder";
                expectThrows(
                        CorruptIndexException.class,
                        () -> copyFileToRepo(fsSourceDir, fileName, localRepoName, destinationFolder, repoFactory, resourceLoader));
                expectThrows(
                        CorruptIndexException.class,
                        () ->
                                copyFileToDir(
                                        fsSourceDir, fileName, destinationDir, localRepoName, repoFactory, resourceLoader));

                // When we copy the encrypted file with the EncryptionBackupRepository,
                // then it succeeds because the checksum is verified in decrypted form,
                // and the file is copied in encrypted form.
                copyFileToRepo(encSourceDir, fileName, encryptionRepoName, destinationFolder, repoFactory, resourceLoader);
                // Check the copy starts with the encryption magic, not the regular codec magic, this means it is encrypted.
                assertEquals(ENCRYPTION_MAGIC, readCodecMagic(fileName, encryptionRepoName, destinationFolder, repoFactory, resourceLoader));
                copyFileToDir(
                        encSourceDir, fileName, destinationDir, encryptionRepoName, repoFactory, resourceLoader);
                // Check the copy starts with the encryption magic, not the regular codec magic, this means it is encrypted.
                assertEquals(ENCRYPTION_MAGIC, readCodecMagic(fileName, destinationDir));

                // When we restore the encrypted copy with the EncryptionBackupRepository,
                // then the file is restored in its encrypted form.
                try (BackupRepository repo = repoFactory.newInstance(resourceLoader, encryptionRepoName)) {
                    URI repoDir = repo.resolve(getBaseUri(), destinationFolder);
                    String restoreFileName = "restore-file";
                    repo.copyIndexFileTo(repoDir, fileName, encSourceDir, restoreFileName);
                    // Check the restored file starts with the encryption magic, not the regular codec magic, this means it is encrypted.
                    assertEquals(ENCRYPTION_MAGIC, readCodecMagic(restoreFileName, fsSourceDir));
                    // Check the restored file checksum to ensure it can be decrypted properly.
                    verifyChecksum(encSourceDir, restoreFileName);
                }
            }
        }
    }

    private PluginInfo getPluginInfo(
            String pluginName, Class<? extends BackupRepository> repoClass, boolean isDefault, NamedList<Object> initArgs) {
        Map<String, String> attrs = new HashMap<>();
        attrs.put(CoreAdminParams.NAME, pluginName);
        attrs.put(FieldType.CLASS_NAME, repoClass.getName());
        attrs.put("default", Boolean.toString(isDefault));
        return new PluginInfo("repository", attrs, initArgs, null);
    }

    private void copyFileToRepo(
            Directory dir,
            String fileName,
            String repoName,
            String destinationFolder,
            BackupRepositoryFactory repoFactory,
            SolrResourceLoader resourceLoader)
            throws IOException, URISyntaxException {
        try (BackupRepository repo = repoFactory.newInstance(resourceLoader, repoName)) {
            URI destinationDir = repo.resolve(getBaseUri(), destinationFolder);
            repo.copyIndexFileFrom(dir, fileName, destinationDir, fileName);
        }
    }

    private void copyFileToDir(
            Directory sourceDir,
            String fileName,
            Directory destinationDir,
            String repoName,
            BackupRepositoryFactory repoFactory,
            SolrResourceLoader resourceLoader)
            throws IOException {
        try (BackupRepository repo = repoFactory.newInstance(resourceLoader, repoName)) {
            repo.copyIndexFileFrom(sourceDir, fileName, destinationDir, fileName);
        }
    }

    private int readCodecMagic(
            String fileName,
            String repoName,
            String destinationFolder,
            BackupRepositoryFactory repoFactory,
            SolrResourceLoader resourceLoader)
            throws IOException, URISyntaxException {
        try (BackupRepository repo = repoFactory.newInstance(resourceLoader, repoName)) {
            URI destinationUri = repo.resolve(getBaseUri(), destinationFolder);
            try (Directory destinationDir = FSDirectory.open(Path.of(destinationUri))) {
                return readCodecMagic(fileName, destinationDir);
            }
        }
    }

    private int readCodecMagic(String fileName, Directory destinationDir) throws IOException {
        try (IndexInput in = destinationDir.openInput(fileName, IOContext.READ)) {
            return CodecUtil.readBEInt(in);
        }
    }

    private void verifyChecksum(Directory sourceDir, String sourceFileName) throws IOException {
        try (ChecksumIndexInput is = sourceDir.openChecksumInput(sourceFileName, DirectoryFactory.IOCONTEXT_NO_CACHE)) {
            long left = is.length() - CodecUtil.footerLength();
            long bufferSize = 8192;
            byte[] buffer = new byte[(int) bufferSize];
            while (left > 0) {
                int toCopy = (int) Math.min(left, bufferSize);
                is.readBytes(buffer, 0, toCopy);
                left -= toCopy;
            }
            // Verify the checksum.
            CodecUtil.checkFooter(is);
        }
    }

    /**
     * Makes {@link #setDelegate} accessible.
     */
    private static class TestEncryptionBackupRepository extends EncryptionBackupRepository {

        @Override
        public void setDelegate(BackupRepository delegate) {
            super.setDelegate(delegate);
        }
    }

    /**
     * Mocks a key secret in the commit metadata.
     */
    private static class TestEncryptionDirectory extends EncryptionDirectory {

        TestEncryptionDirectory(Directory delegate, AesCtrEncrypterFactory encrypterFactory, KeySupplier keySupplier)
                throws IOException {
            super(delegate, encrypterFactory, keySupplier);
        }

        @Override
        protected CommitUserData readLatestCommitUserData() {
            return new CommitUserData("test", Map.of(COMMIT_ACTIVE_KEY, "0"));
        }

        @Override
        protected byte[] getKeySecret(String keyRef) {
            return TestingKeySupplier.KEY_SECRET_1;
        }
    }
}