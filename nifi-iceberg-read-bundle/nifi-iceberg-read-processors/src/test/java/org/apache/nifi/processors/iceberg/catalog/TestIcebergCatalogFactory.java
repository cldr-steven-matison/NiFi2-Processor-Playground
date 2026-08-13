/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.iceberg.catalog;

import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.oauth2.AccessToken;
import org.apache.nifi.oauth2.OAuth2AccessTokenProvider;
import org.apache.nifi.services.iceberg.IcebergCatalogProperty;
import org.apache.nifi.services.iceberg.IcebergCatalogService;
import org.apache.nifi.services.iceberg.IcebergCatalogType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link IcebergCatalogFactory}. Everything reachable without a live catalog
 * endpoint is covered: both HADOOP construction paths, the OAuth-token null-guard on the REST
 * path (the differentiator from the stock factory), and the unsupported-catalog-type error.
 */
public class TestIcebergCatalogFactory {

    @TempDir
    private Path tempDir;

    /** Minimal {@link IcebergCatalogService} stub — only the three methods the factory calls. */
    private static class CatalogServiceStub extends AbstractControllerService implements IcebergCatalogService {
        private final IcebergCatalogType type;
        private final Map<IcebergCatalogProperty, Object> properties;
        private final List<String> configFilePaths;

        CatalogServiceStub(IcebergCatalogType type, Map<IcebergCatalogProperty, Object> properties, List<String> configFilePaths) {
            this.type = type;
            this.properties = properties;
            this.configFilePaths = configFilePaths;
        }

        @Override
        public IcebergCatalogType getCatalogType() {
            return type;
        }

        @Override
        public Map<IcebergCatalogProperty, Object> getCatalogProperties() {
            return properties;
        }

        @Override
        public List<String> getConfigFilePaths() {
            return configFilePaths;
        }
    }

    /** OAuth provider stub returning a caller-supplied (possibly null) access token. */
    private static class OAuthProviderStub extends AbstractControllerService implements OAuth2AccessTokenProvider {
        private final AccessToken accessToken;

        OAuthProviderStub(AccessToken accessToken) {
            this.accessToken = accessToken;
        }

        @Override
        public AccessToken getAccessDetails() {
            return accessToken;
        }
    }

    @Test
    public void testHadoopCatalogWithoutConfigFiles() {
        final IcebergCatalogService service = new CatalogServiceStub(
                IcebergCatalogType.HADOOP,
                Map.of(IcebergCatalogProperty.WAREHOUSE_LOCATION, tempDir.toUri().toString()),
                null);

        final Catalog catalog = new IcebergCatalogFactory(service).create();
        assertInstanceOf(HadoopCatalog.class, catalog);
    }

    @Test
    public void testHadoopCatalogWithConfigFiles() throws IOException {
        final Path coreSite = tempDir.resolve("core-site.xml");
        Files.writeString(coreSite, "<?xml version=\"1.0\"?><configuration></configuration>");

        final IcebergCatalogService service = new CatalogServiceStub(
                IcebergCatalogType.HADOOP,
                Map.of(IcebergCatalogProperty.WAREHOUSE_LOCATION, tempDir.toUri().toString()),
                List.of(coreSite.toString()));

        final Catalog catalog = new IcebergCatalogFactory(service).create();
        assertInstanceOf(HadoopCatalog.class, catalog);
    }

    @Test
    public void testRestCatalogNullTokenThrows() {
        // OAuth provider present but returning no access details → the null-guard must fire
        final IcebergCatalogService service = new CatalogServiceStub(
                IcebergCatalogType.REST,
                Map.of(IcebergCatalogProperty.CATALOG_URI, "http://localhost:8181",
                        IcebergCatalogProperty.OAUTH_TOKEN_SERVICE, new OAuthProviderStub(null)),
                null);

        final IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> new IcebergCatalogFactory(service).create());
        assertTrue(e.getMessage().contains("no access token"));
    }

    @Test
    public void testRestCatalogBlankTokenThrows() {
        final AccessToken blank = new AccessToken("", null, "Bearer", 3600, null);
        final IcebergCatalogService service = new CatalogServiceStub(
                IcebergCatalogType.REST,
                Map.of(IcebergCatalogProperty.CATALOG_URI, "http://localhost:8181",
                        IcebergCatalogProperty.OAUTH_TOKEN_SERVICE, new OAuthProviderStub(blank)),
                null);

        assertThrows(IllegalStateException.class, () -> new IcebergCatalogFactory(service).create());
    }

    @Test
    public void testUnsupportedCatalogTypeThrows() {
        final IcebergCatalogService service = new CatalogServiceStub(
                IcebergCatalogType.HIVE, Map.of(), null);

        assertThrows(UnsupportedOperationException.class, () -> new IcebergCatalogFactory(service).create());
    }
}
