/*
 * Copyright 2019 and onwards Makoto Yui
 *
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
package memserv4j;

import memserv4j.util.io.FileUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public final class Settings {
    private static final Log LOG = LogFactory.getLog(Settings.class);

    private static final String PROPERTY_FILE_NAME = "memserv4j.properties";

    //--------------------------------------------
    // Shared variables

    private static final Properties properties;
    static {
        properties = new Properties();
        final String userDir = System.getProperty("user.home");
        try {
            // put default settings.
            InputStream is = Settings.class.getResourceAsStream(PROPERTY_FILE_NAME);
            properties.load(is);
            // put user specific settings.            
            File propFile = new File(userDir, PROPERTY_FILE_NAME);
            if (propFile.exists()) {
                properties.load(new FileInputStream(propFile));
                LOG.info("Loaded memserv4j.properties in: " + propFile.getAbsolutePath());
            }
        } catch (IOException e) {
            throw new IllegalStateException(
                "Exception caused while loading user provided properties file.", e);
        }
        // import external files
        importExternalFiles(properties, userDir);
    }

    private static void importExternalFiles(final Properties props, final String userDir) {
        final String externals = props.getProperty("prop.external");
        if (externals == null) {
            return;
        }
        String[] filePaths = externals.split(";");
        for (String fp : filePaths) {
            fp = fp.trim();
            if (fp.length() == 0) {
                continue;
            }
            final InputStream is = ClassLoader.getSystemResourceAsStream(fp);
            if (is != null) {
                try {
                    props.load(is);
                    LOG.info("Loaded an external property file in the classpath: " + fp);
                } catch (IOException e) {
                    LOG.warn("An error caused while loading a property file: " + fp);
                }
                String fileName = FileUtils.basename(fp, '/');
                File propFile = new File(userDir, fileName);
                if (propFile.exists()) {
                    try {
                        props.load(new FileInputStream(propFile));
                        LOG.info("Loaded an external property file in the user dir: " + fp);
                    } catch (IOException e) {
                        LOG.warn("An error caused while loading a property file: "
                                + propFile.getAbsolutePath());
                    }
                }
            }
        }
    }

    private Settings() {} // prevent instantiation

    public static Properties getProperties() {
        return properties;
    }

    /**
     * Gets the configuration value from the key.
     */
    public static String get(final String key) {
        return properties.getProperty(key);
    }

    public static String get(final String key, final String defaultValue) {
        return properties.getProperty(key, defaultValue);
    }

    public static String getThroughSystemProperty(final String key) {
        final String v = System.getProperty(key);
        return (v == null) ? properties.getProperty(key) : v;
    }

    /**
     * Puts configuration value.
     */
    public static void put(final String key, final String value) {
        properties.put(key, value);
    }

    /**
     * Overloads the specified properties.
     */
    public static void putAll(Properties props) {
        properties.putAll(props);
    }

}
