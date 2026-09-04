/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.filesystem.hdfs.audit;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.inject.Inject;
import io.trino.filesystem.Location;

import java.util.Optional;
import java.util.regex.Pattern;

import static io.trino.filesystem.hdfs.audit.HdfsOperationAuditPathMode.HASH;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class HdfsAuditPathSanitizer
{
    private static final HashFunction SHA_256 = Hashing.sha256();
    private static final Pattern URI = Pattern.compile("(?i)[a-z][a-z0-9+.-]*://[^\\s]+");
    private static final Pattern URI_USER_INFO = Pattern.compile("(?i)([a-z][a-z0-9+.-]*://)[^\\s/@]+@");
    private static final Pattern URI_QUERY_OR_FRAGMENT = Pattern.compile("(?i)([a-z][a-z0-9+.-]*://[^\\s?#]*)(?:[?#][^\\s]*)");

    private final HdfsOperationAuditPathMode pathMode;
    private final int errorMessageMaxLength;

    @Inject
    public HdfsAuditPathSanitizer(HdfsOperationAuditConfig config)
    {
        requireNonNull(config, "config is null");
        this.pathMode = config.getPathMode();
        this.errorMessageMaxLength = config.getErrorMessageMaxLength();
    }

    public String sanitize(Location location)
    {
        String sanitizedPath = fullSanitizedPath(requireNonNull(location, "location is null"));
        if (pathMode == HASH) {
            return "sha256:" + SHA_256.hashString(sanitizedPath, UTF_8);
        }
        return sanitizedPath;
    }

    public Optional<String> sanitizeErrorMessage(Throwable failure, Location source, Optional<Location> target)
    {
        requireNonNull(failure, "failure is null");
        requireNonNull(source, "source is null");
        requireNonNull(target, "target is null");

        if (failure.getMessage() == null || errorMessageMaxLength == 0) {
            return Optional.empty();
        }

        String message = failure.getMessage().replace(source.toString(), sanitize(source));
        if (target.isPresent()) {
            message = message.replace(target.orElseThrow().toString(), sanitize(target.orElseThrow()));
        }
        if (pathMode == HASH) {
            message = URI.matcher(message).replaceAll("[uri-redacted]");
        }
        else {
            message = URI_USER_INFO.matcher(message).replaceAll("$1");
            message = URI_QUERY_OR_FRAGMENT.matcher(message).replaceAll("$1");
        }
        message = removeControlCharacters(message);
        if (message.length() > errorMessageMaxLength) {
            message = message.substring(0, errorMessageMaxLength);
        }
        return Optional.of(message);
    }

    private static String fullSanitizedPath(Location location)
    {
        String path = stripQueryAndFragment(removeControlCharacters(location.path()));
        if (location.scheme().isEmpty()) {
            return "/" + path;
        }

        String scheme = removeControlCharacters(location.scheme().orElseThrow());
        StringBuilder builder = new StringBuilder(scheme).append("://");
        location.host().map(HdfsAuditPathSanitizer::removeControlCharacters).ifPresent(builder::append);
        location.port().ifPresent(port -> builder.append(':').append(port));
        return builder.append('/').append(path).toString();
    }

    private static String stripQueryAndFragment(String value)
    {
        int queryIndex = value.indexOf('?');
        int fragmentIndex = value.indexOf('#');
        int endIndex;
        if (queryIndex < 0) {
            endIndex = fragmentIndex;
        }
        else if (fragmentIndex < 0) {
            endIndex = queryIndex;
        }
        else {
            endIndex = Math.min(queryIndex, fragmentIndex);
        }
        return endIndex < 0 ? value : value.substring(0, endIndex);
    }

    private static String removeControlCharacters(String value)
    {
        StringBuilder builder = new StringBuilder(value.length());
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            builder.append(Character.isISOControl(character) ? ' ' : character);
        }
        return builder.toString();
    }
}
