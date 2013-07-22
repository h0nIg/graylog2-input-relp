/**
 * Copyright 2013 Hans-Joachim Kliemeck <git@kliemeck.de>
 *
 * graylog2-input-relp is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * graylog2-input-relp is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with graylog2-input-relp.  If not, see <http://www.gnu.org/licenses/>.
 *
 */
package de.kliemeck.graylog2.relp;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.net.InetSocketAddress;
import org.graylog2.plugin.GraylogServer;
import org.graylog2.plugin.inputs.MessageInput;
import org.graylog2.plugin.inputs.MessageInputConfigurationException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.channel.socket.oio.OioServerSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RELPInput implements MessageInput {

    public static final String NAME = "relp input";
    private static final Logger LOG = LoggerFactory.getLogger(RELPInput.class);

    private InetSocketAddress socketAddress;

    @Override
    public void initialize(Map<String, String> configuration, GraylogServer graylogServer) throws MessageInputConfigurationException {
        socketAddress = new InetSocketAddress(
                configuration.get("listen_address"),
                Integer.parseInt(configuration.get("listen_port"))
        );

        final ExecutorService bossThreadPool = Executors.newCachedThreadPool(
                new ThreadFactoryBuilder()
                .setNameFormat("input-relp-boss-%d")
                .build());
        
        final ExecutorService workerThreadPool = Executors.newCachedThreadPool(
                new ThreadFactoryBuilder()
                .setNameFormat("input-relp-worker-%d")
                .build());

        ServerBootstrap tcpBootstrap = new ServerBootstrap(
            new OioServerSocketChannelFactory(bossThreadPool, workerThreadPool)
        );

        tcpBootstrap.setPipelineFactory(new RELPPipelineFactory(graylogServer));

        try {
            tcpBootstrap.bind(socketAddress);
        } catch (ChannelException e) {
            LOG.error("Could not bind RELP input {}", socketAddress, e);
        }
    }

    @Override
    public Map<String, String> getRequestedConfiguration() {
        return new HashMap<String, String>() {
            {
                put("listen_port", "The port to listen on");
                put("listen_address", "The address to listen on");
            }
        };
    }

    @Override
    public String getName() {
        return NAME;
    }
}
