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
package brooklyn.entity.network.bind;

import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import brooklyn.entity.basic.AbstractSoftwareProcessSshDriver;
import brooklyn.entity.basic.lifecycle.ScriptHelper;
import brooklyn.location.basic.SshMachineLocation;
import brooklyn.util.collections.MutableMap;
import brooklyn.util.net.Networking;
import brooklyn.util.net.Protocol;
import brooklyn.util.ssh.BashCommands;
import brooklyn.util.ssh.IptablesCommands;
import brooklyn.util.ssh.IptablesCommands.Chain;
import brooklyn.util.ssh.IptablesCommands.Policy;
import brooklyn.util.text.Strings;

import com.google.common.collect.ImmutableList;

public class BindDnsServerSshDriver extends AbstractSoftwareProcessSshDriver implements BindDnsServerDriver {

    private static final Logger LOG = LoggerFactory.getLogger(BindDnsServerSshDriver.class);
    private String serviceName = "named";

    public BindDnsServerSshDriver(BindDnsServerImpl entity, SshMachineLocation machine) {
        super(entity, machine);
    }

    @Override
    public BindDnsServerImpl getEntity() {
        return (BindDnsServerImpl) super.getEntity();
    }

    @Override
    public void install() {
        List<String> commands = ImmutableList.<String>builder()
                .add(BashCommands.installPackage(MutableMap.of(
                        "yum", "bind", "apt", "bind9"), "bind"))
                .add(BashCommands.ok("which setenforce && " + BashCommands.sudo("setenforce 0")))
                .build();
        newScript(INSTALLING)
                .failOnNonZeroResultCode()
                .body.append(commands)
                .execute();

        ScriptHelper s = newScript("Test to determine service named")
                .gatherOutput()
                .noExtraOutput()
                .body.append("if service --status-all 2>&1 | grep -q bind9; then echo bind9; else echo named; fi");
        s.execute();
        serviceName = s.getResultStdout().trim();
    }

    @Override
    public void customize() {
        Integer dnsPort = getEntity().getDnsPort();
        Map<String, Object> ports = MutableMap.<String, Object>of("dnsPort", dnsPort);
        Networking.checkPortsValid(ports);
        newScript(CUSTOMIZING)
                .body.append(
                        // TODO determine name of ethernet interface if not eth0?
                        IptablesCommands.insertIptablesRule(Chain.INPUT, "eth0", Protocol.UDP, dnsPort, Policy.ACCEPT),
                        IptablesCommands.insertIptablesRule(Chain.INPUT, "eth0", Protocol.TCP, dnsPort, Policy.ACCEPT),
                        BashCommands.sudo("service iptables save"),
                        BashCommands.sudo("service iptables restart")
                ).execute();
    }

    @Override
    public void launch() {
        newScript(MutableMap.of("usePidFile", false), LAUNCHING)
                .body.append(BashCommands.sudo("service "+serviceName+" start"))
                .execute();
    }

    @Override
    public boolean isRunning() {
        return newScript(MutableMap.of("usePidFile", false), CHECK_RUNNING)
                .body.append(BashCommands.sudo("service "+serviceName+" status"))
                .execute() == 0;
    }

    @Override
    public void stop() {
        newScript(MutableMap.of("usePidFile", false), STOPPING)
                .body.append(BashCommands.sudo("service "+serviceName+" stop"))
                .execute();
    }

    @Override
    public void updateBindConfiguration() {
        final boolean updatedNamed = copyAsRoot(entity.getConfig(BindDnsServer.NAMED_CONF_TEMPLATE), "/etc/named.conf");
        final boolean updatedDomain = copyAsRoot(entity.getConfig(BindDnsServer.DOMAIN_ZONE_FILE_TEMPLATE), "/var/named/domain.zone");
        final boolean updatedReverse = copyAsRoot(entity.getConfig(BindDnsServer.REVERSE_ZONE_FILE_TEMPLATE), "/var/named/reverse.zone");

        final int result = getMachine().execScript("restart bind", ImmutableList.of(BashCommands.sudo("service "+serviceName+" restart")));

        if (!updatedNamed) {
            LOG.warn("Failed to update named configuration for '{}'. Failed to copy file on {}.", entity.getConfig(BindDnsServer.DOMAIN_NAME), entity);
        }
        if (!updatedDomain) {
            LOG.warn("Failed to update zone file for '{}'. Failed to copy file on {}.", entity.getConfig(BindDnsServer.DOMAIN_NAME), entity);
        }
        if (!updatedReverse) {
            LOG.warn("Failed to update reverse zone file for '{}'. Failed to copy file on {}.", entity.getConfig(BindDnsServer.DOMAIN_NAME), entity);
        }

        LOG.info("restarted BIND server with updated config for '{}' on {} (exit code {}).",
                new Object[]{entity.getConfig(BindDnsServer.DOMAIN_NAME), entity, result});
    }

    private boolean copyAsRoot(String template, String destination) {
        String content = processTemplate(template);
        String temp = "/tmp/template-" + Strings.makeRandomId(6);
        final int result = getMachine().copyTo(new ByteArrayInputStream(content.getBytes()), temp);
        if (result == 0) {
            return getMachine().execScript("copying file", ImmutableList.of(BashCommands.sudo(String.format("mv %s %s", temp, destination)))) == 0;
        }
        else {
            return false;
        }
    }

}
