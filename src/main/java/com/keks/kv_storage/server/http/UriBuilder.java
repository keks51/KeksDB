package com.keks.kv_storage.server.http;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;


public class UriBuilder {

    private final String schema;
    private final String host;
    private final int port;
    private final List<String> pathSegments = new ArrayList<>();
    private final List<KeyValueParameter> uriParams = new ArrayList<>();

    public UriBuilder(String schema, String host, int port) {
        this.schema = schema;
        this.host = host;
        this.port = port;
    }

    public URI build() throws URISyntaxException {
        return new URI(buildString());
    }

    private String buildString() {
        final StringBuilder sb = new StringBuilder();
        if (this.schema != null) {
            sb.append(this.schema).append(':');
        }

        if (this.host != null) {
            sb.append("//");
            sb.append(this.host);
            if (this.port >= 0) {
                sb.append(":").append(this.port);
            }
        }
        if (!this.pathSegments.isEmpty()) {
            for (String pathSegment : pathSegments) {
                sb.append("/").append(pathSegment);
            }

        }
        if (!this.uriParams.isEmpty()) {
            sb.append("?");
            boolean appendSep = false;
            for (KeyValueParameter keyValueParameter : uriParams) {
                if (appendSep) sb.append("&");
                appendSep = true;
                sb.append(keyValueParameter);
            }
        }

        return sb.toString();
    }

    public UriBuilder setPathSegments(final String... pathSegments) {
        if (pathSegments.length > 0) {
            this.pathSegments.addAll(Arrays.asList(pathSegments));
        }
        return this;
    }

    public UriBuilder addParameters(final List<KeyValueParameter> nvps) {
        this.uriParams.addAll(nvps);
        return this;
    }

    public UriBuilder setParameters(final KeyValueParameter... nvps) {
        this.uriParams.clear();
        Collections.addAll(this.uriParams, nvps);
        return this;
    }

    public UriBuilder addParameter(final String param, final Object value) {
        this.uriParams.add(new KeyValueParameter(param, value.toString()));
        return this;
    }

    public UriBuilder addParameters(Map<String, Object> params) {
        for (Map.Entry<String, Object> entry : params.entrySet()) {
            this.uriParams.add(new KeyValueParameter(entry.getKey(), entry.getValue().toString()));
        }
        return this;
    }

    public UriBuilder setParameter(final String param, final String value) {
        if (!this.uriParams.isEmpty()) {
            this.uriParams.removeIf(nvp -> nvp.key.equals(param));
        }
        this.uriParams.add(new KeyValueParameter(param, value));
        return this;
    }

    public UriBuilder clearParameters() {
        this.uriParams.clear();
        return this;
    }

    public String getSchema() {
        return this.schema;
    }

    public String getHost() {
        return this.host;
    }

    public int getPort() {
        return this.port;
    }

    public List<String> getPathSegments() {
        return new ArrayList<>(this.pathSegments);
    }

    public String getPath() {
        final StringBuilder result = new StringBuilder();
        for (final String segment : this.pathSegments) {
            result.append('/').append(segment);
        }
        return result.toString();
    }


    public List<KeyValueParameter> getUriParams() {
        return new ArrayList<>(this.uriParams);
    }

    public UriBuilder copy() {
        return new UriBuilder(schema, host, port);
    }

    @Override
    public String toString() {
        return buildString();
    }

    static class KeyValueParameter {

        public final String key;
        public final String value;

        public KeyValueParameter(final String key, final String value) {
            super();
            this.key = key;
            this.value = value;
        }

        @Override
        public String toString() {
            return this.key + "=" + this.value;
        }

    }


}

