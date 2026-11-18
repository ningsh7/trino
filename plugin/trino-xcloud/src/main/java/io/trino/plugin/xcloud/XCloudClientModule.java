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

import com.bonc.xcloud.jdbc.XCloudDriver;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.log.Logger;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.plugin.jdbc.BaseJdbcConfig;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.DriverConnectionFactory;
import io.trino.plugin.jdbc.ForBaseJdbc;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.credential.CredentialProvider;

import java.time.ZoneId;
import java.time.ZoneOffset;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class XCloudClientModule
        extends AbstractConfigurationAwareModule
{
    @Override
    public void setup(Binder binder)
    {
        binder.bind(JdbcClient.class).annotatedWith(ForBaseJdbc.class).to(XCloudClient.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(XCloudJdbcConfig.class);
        com.google.inject.multibindings.OptionalBinder.newOptionalBinder(binder, io.trino.plugin.jdbc.QueryBuilder.class).setBinding().to(XCloudQueryBuilder.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    @ForBaseJdbc
    public static ConnectionFactory connectionFactory(BaseJdbcConfig config, CredentialProvider credentials, OpenTelemetry telemetry)
    {
        checkState(ZoneId.systemDefault().normalized().equals(ZoneOffset.UTC), "XCloud JDBC requires -Duser.timezone=UTC on coordinator and workers to preserve DATE/TIMESTAMP values");
        XCloudDriver driver = new XCloudDriver();
        Logger.get(XCloudClientModule.class).info(
                "XCloud connector: driver=2.14.7 (%s.%s), source=%s, JDK=%s; JDBC commit/rollback are no-ops; writes are not retried",
                driver.getMajorVersion(),
                driver.getMinorVersion(),
                XCloudDriver.class.getProtectionDomain().getCodeSource().getLocation(),
                Runtime.version());
        return DriverConnectionFactory.builder(driver, config.getConnectionUrl(), credentials)
                .setOpenTelemetry(telemetry)
                .build();
    }
}
