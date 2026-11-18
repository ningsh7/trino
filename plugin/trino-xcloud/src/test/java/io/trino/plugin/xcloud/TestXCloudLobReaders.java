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
package io.trino.plugin.xcloud;

import io.trino.plugin.jdbc.SliceReadFunction;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.Reader;
import java.sql.Clob;
import java.sql.ResultSet;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestXCloudLobReaders
{
    @Test
    public void testOnlyImplementedCharArrayOverloadIsUsed()
            throws Exception
    {
        String text = "中文🙂".repeat(9000);
        AtomicBoolean closed = new AtomicBoolean();
        Reader reader = new Reader()
        {
            private int offset;

            @Override
            public int read(char[] buffer)
            {
                if (offset == text.length()) {
                    return -1;
                }
                int length = Math.min(137, text.length() - offset);
                text.getChars(offset, offset + length, buffer, 0);
                offset += length;
                return length;
            }

            @Override
            public int read(char[] buffer, int offset, int length)
            {
                throw new UnsupportedOperationException("Native three-argument read is unimplemented");
            }

            @Override
            public void close()
            {
                closed.set(true);
            }
        };
        assertThat(read(reader)).isEqualTo(text);
        assertThat(closed.get()).isTrue();
        assertThatThrownBy(() -> new com.bonc.xcloud.type.XCloudClobReader(null).read(new char[8], 0, 8)).isInstanceOf(RuntimeException.class).hasMessageContaining("Not Implemented");
    }

    @Test
    public void testReadFailureClosesAndPropagates()
            throws Exception
    {
        AtomicBoolean closed = new AtomicBoolean();
        Reader reader = new Reader()
        {
            @Override
            public int read(char[] buffer)
                    throws IOException
            {
                throw new IOException("read-failure");
            }

            @Override
            public int read(char[] buffer, int offset, int length)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public void close()
            {
                closed.set(true);
            }
        };
        assertThatThrownBy(() -> read(reader)).hasRootCauseMessage("read-failure");
        assertThat(closed.get()).isTrue();
        assertThat(read(Reader.of(""))).isEmpty();
    }

    private static String read(Reader reader)
            throws Exception
    {
        Clob clob = mock(Clob.class);
        when(clob.getCharacterStream()).thenReturn(reader);
        ResultSet result = mock(ResultSet.class);
        when(result.getClob(1)).thenReturn(clob);
        return ((SliceReadFunction) TestXCloudTypes.mapping("CLOB", 0, 0).getReadFunction()).readSlice(result, 1).toStringUtf8();
    }
}
