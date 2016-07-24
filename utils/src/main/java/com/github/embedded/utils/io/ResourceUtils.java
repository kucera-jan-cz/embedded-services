package com.github.embedded.utils.io;

import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class ResourceUtils {
    private static final Pattern URLPattern = Pattern.compile("(?<scheme>[a-zA-Z](\\w+|\\+|\\.|\\-)*:)?(?<rest>.*)");

    private ResourceUtils() {
    }

    public static String asText(String path) throws IOException {
        return asText(path, StandardCharsets.UTF_8);
    }

    public static String asText(String path, Charset charset) throws IOException {
        return IOUtils.toString(asInputStream(path), charset);
    }

    public static InputStream asInputStream(String path) throws IOException {
        Matcher matcher = URLPattern.matcher(path);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid URI: " + path);
        }
        String scheme = matcher.group("scheme").toLowerCase();
        String restOfUrl = matcher.group("rest");
        URL url = toUrl(path, scheme, restOfUrl);
        return url.openStream();
    }

    public static URL toUrl(String path, String scheme, String restOfUrl) {
        try {
            switch (scheme) {
                case "file:":
                    return new URL(path);
                case "classpath:":
                    return ResourceUtils.class.getClassLoader().getResource(restOfUrl);
                default:
                    return ResourceUtils.class.getClassLoader().getResource(path);
            }
        } catch (MalformedURLException ex) {
            throw new IllegalArgumentException("Invalid URI: " + path, ex);
        }
    }
}
