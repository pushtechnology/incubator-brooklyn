/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package brooklyn.entity.database.crate;

import brooklyn.config.ConfigKey;
import brooklyn.entity.basic.Attributes;
import brooklyn.entity.basic.ConfigKeys;
import brooklyn.entity.basic.SoftwareProcess;
import brooklyn.entity.database.DatastoreMixins.DatastoreCommon;
import brooklyn.entity.java.UsesJava;
import brooklyn.entity.java.UsesJavaMXBeans;
import brooklyn.entity.java.UsesJmx;
import brooklyn.entity.proxying.ImplementedBy;
import brooklyn.event.AttributeSensor;
import brooklyn.event.basic.AttributeSensorAndConfigKey;
import brooklyn.event.basic.BasicAttributeSensorAndConfigKey;
import brooklyn.event.basic.PortAttributeSensorAndConfigKey;
import brooklyn.event.basic.Sensors;
import brooklyn.location.basic.PortRanges;
import brooklyn.util.flags.SetFromFlag;

@ImplementedBy(CrateNodeImpl.class)
public interface CrateNode extends SoftwareProcess, UsesJava,UsesJmx, UsesJavaMXBeans, DatastoreCommon {

    @SetFromFlag("version")
    ConfigKey<String> SUGGESTED_VERSION = ConfigKeys.newConfigKeyWithDefault(SoftwareProcess.SUGGESTED_VERSION,
            "0.45.7");

    @SetFromFlag("downloadUrl")
    AttributeSensorAndConfigKey<String, String> DOWNLOAD_URL = new BasicAttributeSensorAndConfigKey.StringAttributeSensorAndConfigKey(
            Attributes.DOWNLOAD_URL,
            "https://cdn.crate.io/downloads/releases/crate-${version}.tar.gz");

    @SetFromFlag("serverConfig")
    BasicAttributeSensorAndConfigKey<String> SERVER_CONFIG_URL = new BasicAttributeSensorAndConfigKey.StringAttributeSensorAndConfigKey(
            "crate.serverConfig", "A URL of a YAML file to use to configure the server",
            "classpath://brooklyn/entity/database/crate/crate.yaml");

    @SetFromFlag("port")
    public static final PortAttributeSensorAndConfigKey CRATE_PORT = new PortAttributeSensorAndConfigKey(
            "crate.port", "The port for node-to-node communication", PortRanges.fromString("4300+"));

    @SetFromFlag("httpPort")
    public static final PortAttributeSensorAndConfigKey CRATE_HTTP_PORT = new PortAttributeSensorAndConfigKey(
            "crate.httpPort", "The port for HTTP traffic", PortRanges.fromString("4200+"));

    AttributeSensor<String> MANAGEMENT_URL = Sensors.newStringSensor(
            "crate.managementUri", "The address at which the Crate server listens");

    AttributeSensor<String> SERVER_NAME = Sensors.newStringSensor(
            "crate.server.name", "The name of the server");

    AttributeSensor<Boolean> SERVER_OK = Sensors.newBooleanSensor(
            "crate.server.ok", "True if the server reports thus");

    AttributeSensor<Integer> SERVER_STATUS = Sensors.newIntegerSensor(
            "crate.server.status", "The status of the server");

    AttributeSensor<String> SERVER_BUILD_TIMESTAMP = Sensors.newStringSensor(
            "crate.server.buildTimestamp", "The timestamp of the server build");

    AttributeSensor<String> SERVER_BUILD_HASH = Sensors.newStringSensor(
            "crate.server.buildHash", "The build hash of the server");

    AttributeSensor<Boolean> SERVER_IS_BUILD_SNAPSHOT = Sensors.newBooleanSensor(
            "crate.server.isBuildSnapshot", "True if the server reports it is a snapshot build");

    AttributeSensor<String> SERVER_LUCENE_VERSION = Sensors.newStringSensor(
            "crate.server.luceneVersion", "The Lucene version of the server");

    AttributeSensor<String> SERVER_ES_VERSION = Sensors.newStringSensor(
            "crate.server.esVersion", "The ES version of the server");

}