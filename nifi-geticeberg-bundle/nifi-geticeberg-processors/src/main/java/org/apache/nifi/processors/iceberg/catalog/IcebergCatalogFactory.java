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

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.nifi.oauth2.AccessToken;
import org.apache.nifi.oauth2.OAuth2AccessTokenProvider;
import org.apache.nifi.processors.iceberg.IcebergUtils;
import org.apache.nifi.services.iceberg.IcebergCatalogProperty;
import org.apache.nifi.services.iceberg.IcebergCatalogService;

import java.util.HashMap;
import java.util.Map;

/**
 * Builds an Iceberg {@link Catalog} from the properties exposed by an {@link IcebergCatalogService}.
 * Read-oriented port of the CFM factory: supports REST and HADOOP catalogs only. Unlike the stock
 * factory it null-guards the OAuth token (a disabled/failed provider fails with a clear error
 * instead of an NPE deep inside Iceberg's EnvironmentUtil) and requests vended credentials on the
 * REST path.
 */
public class IcebergCatalogFactory {

    private static final String ACCESS_DELEGATION_HEADER = "header.X-Iceberg-Access-Delegation";
    private static final String VENDED_CREDENTIALS = "vended-credentials";

    private final IcebergCatalogService catalogService;
    private final Map<String, String> additionalProperties;

    public IcebergCatalogFactory(IcebergCatalogService catalogService) {
        this(catalogService, Map.of());
    }

    public IcebergCatalogFactory(IcebergCatalogService catalogService, Map<String, String> additionalProperties) {
        this.catalogService = catalogService;
        this.additionalProperties = additionalProperties;
    }

    public Catalog create() {
        return switch (catalogService.getCatalogType()) {
            case HADOOP -> initHadoopCatalog(catalogService);
            case REST -> initRestCatalog(catalogService);
            default -> throw new UnsupportedOperationException(
                    "Catalog type " + catalogService.getCatalogType() + " is not supported by GetIceberg (REST and HADOOP only)");
        };
    }

    private Catalog initHadoopCatalog(IcebergCatalogService catalogService) {
        final Map<IcebergCatalogProperty, Object> catalogProperties = catalogService.getCatalogProperties();
        final String warehousePath = (String) catalogProperties.get(IcebergCatalogProperty.WAREHOUSE_LOCATION);

        if (catalogService.getConfigFilePaths() != null) {
            return new HadoopCatalog(IcebergUtils.getConfigurationFromFiles(catalogService.getConfigFilePaths()), warehousePath);
        } else {
            return new HadoopCatalog(new Configuration(), warehousePath);
        }
    }

    private Catalog initRestCatalog(IcebergCatalogService catalogService) {
        final Configuration configuration = IcebergUtils.getConfigurationFromFiles(catalogService.getConfigFilePaths());
        final Map<IcebergCatalogProperty, Object> catalogProperties = catalogService.getCatalogProperties();

        final Map<String, String> properties = new HashMap<>();
        putIfPresent(properties, "uri", (String) catalogProperties.get(IcebergCatalogProperty.CATALOG_URI));
        putIfPresent(properties, "warehouse", (String) catalogProperties.get(IcebergCatalogProperty.WAREHOUSE_LOCATION));

        if (catalogProperties.containsKey(IcebergCatalogProperty.OAUTH_TOKEN_SERVICE)) {
            final OAuth2AccessTokenProvider oauthService = (OAuth2AccessTokenProvider) catalogProperties.get(IcebergCatalogProperty.OAUTH_TOKEN_SERVICE);
            final AccessToken accessDetails = oauthService == null ? null : oauthService.getAccessDetails();
            final String token = accessDetails == null ? null : accessDetails.getAccessToken();
            if (token == null || token.isBlank()) {
                throw new IllegalStateException("The configured OAuth2 token provider returned no access token; check that the provider is enabled and its credentials are valid");
            }
            properties.put("token", token);
        }

        properties.put(ACCESS_DELEGATION_HEADER, VENDED_CREDENTIALS);
        properties.putAll(additionalProperties);

        final RESTCatalog catalog = new RESTCatalog();
        catalog.setConf(configuration);
        catalog.initialize("rest-catalog", properties);
        return catalog;
    }

    private static void putIfPresent(Map<String, String> properties, String key, String value) {
        if (value != null && !value.isBlank()) {
            properties.put(key, value);
        }
    }
}
