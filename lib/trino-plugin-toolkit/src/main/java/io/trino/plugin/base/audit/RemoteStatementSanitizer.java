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
package io.trino.plugin.base.audit;

import io.trino.plugin.base.audit.RemoteOperationAuditConfig.StatementMode;
import jakarta.annotation.Nullable;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

import static com.google.common.hash.Hashing.sha256;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class RemoteStatementSanitizer
{
    private static final Pattern CONTROL = Pattern.compile("[\\p{Cntrl}\\p{Cf}\\p{Zl}\\p{Zp}]");
    private static final Pattern URI_CREDENTIALS = Pattern.compile("(?i)([a-z][a-z0-9+.-]*://)[^\\s/'\"]+@");
    private static final Pattern URI_PARAMETERS = Pattern.compile("(?i)([a-z][a-z0-9+.-]*://[^\\s?'\"#]+)[?#][^\\s'\"]*");
    // Credential-bearing statements are suppressed as a whole. Do not attempt to parse vendor SQL dialects.
    private static final Pattern CREDENTIAL = Pattern.compile("(?i)\\b(password|passwd|pwd|token|secret|authorization|credential|identified\\s+by)\\b");
    private static final Set<String> TYPES = Set.of("SELECT", "INSERT", "UPDATE", "DELETE", "MERGE", "CREATE", "ALTER", "DROP", "TRUNCATE", "CALL", "SHOW", "EXPLAIN");

    private RemoteStatementSanitizer() {}

    @Nullable
    public static String statement(@Nullable String value, StatementMode mode, int maxLength)
    {
        if (value == null || mode == StatementMode.NONE) {
            return null;
        }
        String cleaned = clean(value);
        cleaned = URI_CREDENTIALS.matcher(cleaned).replaceAll("$1");
        cleaned = URI_PARAMETERS.matcher(cleaned).replaceAll("$1");
        if (CREDENTIAL.matcher(cleaned).find()) {
            cleaned = "[REDACTED CREDENTIAL STATEMENT]";
        }
        if (mode == StatementMode.HASH) {
            return "sha256:" + sha256().hashString(cleaned, UTF_8);
        }
        return truncate(cleaned, maxLength);
    }

    public static String clean(String value)
    {
        return CONTROL.matcher(value).replaceAll(" ");
    }

    public static String truncate(String value, int maxLength)
    {
        return value.substring(0, Math.min(value.length(), maxLength));
    }

    public static String statementType(@Nullable String statement)
    {
        if (statement == null) {
            return "UNKNOWN";
        }
        String first = statement.stripLeading().split("[^a-zA-Z]", 2)[0].toUpperCase(Locale.ENGLISH);
        return TYPES.contains(first) ? first : "UNKNOWN";
    }

    @Nullable
    public static String endpoint(@Nullable String value)
    {
        if (value == null) {
            return null;
        }
        try {
            URI uri = new URI(clean(value));
            if (uri.getScheme() == null || uri.getHost() == null) {
                return null;
            }
            return new URI(uri.getScheme(), null, uri.getHost(), uri.getPort(), uri.getPath(), null, null).toString();
        }
        catch (URISyntaxException e) {
            return null;
        }
    }
}
