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

import java.lang.reflect.Constructor;
import org.graylog2.plugin.GraylogServer;
import org.jboss.netty.channel.ChannelHandler;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;

/**
 * @author Hans-Joachim Kliemeck <git@kliemeck.de>
 */
public class RELPPipelineFactory implements ChannelPipelineFactory {

    private GraylogServer server;

    public RELPPipelineFactory(GraylogServer server) {
        this.server = server;
    }

    @Override
    public ChannelPipeline getPipeline() throws Exception {
        ChannelPipeline p = Channels.pipeline();
        p.addLast("framer", new RELPFrameDecoder());
        p.addLast("relphandler", new RELPChannelHandler());

        // dirty hack
        Class dispatcher = Class.forName("org.graylog2.inputs.syslog.SyslogDispatcher");
        Class core = Class.forName("org.graylog2.Core");
        Constructor constructur = dispatcher.getConstructor(core);

        p.addLast("handler", (ChannelHandler) constructur.newInstance(core.cast(server)));
        return p;
    }
}
