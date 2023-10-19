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
package org.apache.solr.crossdc;

import org.apache.commons.io.FileUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.core.SolrCoreInitializationException;
import org.apache.solr.update.processor.MirroringUpdateRequestProcessorFactory;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Unit test validating that {@link MirroringUpdateRequestProcessorFactory} responds appropriately in Solr is running in standalone mode.
 */
public class CrossDCProducerSolrStandaloneTest extends SolrTestCaseJ4 {

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        h.close();
    }

    @Test
    public void testSolrStandaloneQuietlyNoopsDisabledProducer() throws Exception {
        try (final EmbeddedSolrServer server = createProducerCoreWithProperties("solrconfig-producerdisabled.xml", "producerDisabledCore")) {
            try (final var producerDisabledCore = server.getCoreContainer().getCore("producerDisabledCore")) {
                assertNotNull(producerDisabledCore);
                final var mirrorChain = producerDisabledCore.getUpdateProcessingChain("mirrorUpdateChain");
                assertNotNull(mirrorChain);
                final var updateProcessorList = mirrorChain.getProcessors();
                final var mirroringFactoryOption = updateProcessorList.stream()
                        .filter(pf -> pf instanceof MirroringUpdateRequestProcessorFactory)
                        .findFirst();
                assertTrue("No mirroring factory found in " + updateProcessorList, mirroringFactoryOption.isPresent());
                final var mirroringFactory = mirroringFactoryOption.get();

                final var mirroringProcessorInstance = mirroringFactory.getInstance(null, null, null);
                assertEquals(MirroringUpdateRequestProcessorFactory.NoOpUpdateRequestProcessor.class, mirroringProcessorInstance.getClass());
            }
        }
    }

    @Test
    public void testEnabledProcessorFailsCoreInitInSolrStandalone() throws Exception {
        try (final EmbeddedSolrServer server = createProducerCoreWithProperties("solrconfig.xml", "producerEnabledCore")) {
            expectThrows(SolrCoreInitializationException.class, () -> {
                final var core = server.getCoreContainer().getCore("producerEnabledCore");
                // Should be preempted by exception in line above, but ensures the core is closed in case the test is about to fail
                core.close();
            });
        }
    }

    private static EmbeddedSolrServer createProducerCoreWithProperties(String solrConfigName, String coreName) throws Exception {
        Path tmpHome = createTempDir("tmp-home");
        Path coreDir = tmpHome.resolve(coreName);
        populateCoreDirectory("src/test/resources/configs/cloud-minimal/conf", solrConfigName, coreDir.toFile());
        initCore(
                "solrconfig.xml", "schema.xml", tmpHome.toAbsolutePath().toString(), coreName);

        return new EmbeddedSolrServer(h.getCoreContainer(), coreName);
    }

    /**
     * Copy configset files to a specified location
     *
     * @param sourceLocation the location of schema and solrconfig files to copy
     * @param solrConfigName the name of the solrconfig file to use for this core
     * @param coreDirectory an empty preexisting location use as a core directory.
     * @throws IOException
     */
    private static void populateCoreDirectory(String sourceLocation, String solrConfigName, File coreDirectory) throws IOException {
        File subHome = new File(coreDirectory, "conf");
        if (! coreDirectory.exists()) {
            assertTrue("Failed to make subdirectory ", coreDirectory.mkdirs());
        }
        Files.createFile(coreDirectory.toPath().resolve("core.properties"));
        FileUtils.copyFile(new File(sourceLocation, "schema.xml"), new File(subHome, "schema.xml"));
        FileUtils.copyFile(new File(sourceLocation, solrConfigName), new File(subHome, "solrconfig.xml"));
    }
}
