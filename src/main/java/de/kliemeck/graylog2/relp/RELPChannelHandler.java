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

import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;

/**
 * @author Hans-Joachim Kliemeck <git@kliemeck.de>
 */
public class RELPChannelHandler extends SimpleChannelHandler {
    private static final Pattern framePattern = Pattern.compile("^(\\d{1,9}) ([a-zA-Z]{1,32}) (\\d{1,9} (.+)|0)", Pattern.DOTALL);
    private static final List<String> serverCommands = Arrays.asList("syslog");
    private static final Charset charset = Charset.forName("UTF-8");

    private boolean alreadyOpened = false;
    private int transactionNumber = 1;

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        String message = (String) e.getMessage();
        Matcher matcher = framePattern.matcher(message);
        if (!matcher.matches()) {
            // something went wrong
            e.getChannel().close();
            return;
        }

        String clientTransactionNumber = matcher.group(1);
        String clientCommand = matcher.group(2);
        String clientData = matcher.group(4);

        // prevent nullpointer
        if (clientData == null) {
            clientData = "";
        }

        if (Integer.parseInt(clientTransactionNumber) != transactionNumber) {
            e.getChannel().close();
            return;
        }

        if (!alreadyOpened) {
            if (!clientCommand.equals("open")) {
                e.getChannel().close();
                return;
            }

            Properties inputProperties = new Properties();
            inputProperties.load(new StringReader(clientData));
            if (!inputProperties.containsKey("relp_version")) {
                e.getChannel().close();
                return;
            }

            // remove all commands except the supported ones
            List<String> clientCommands = new ArrayList<String>(Arrays.asList(inputProperties.getProperty("commands").split(",")));
            clientCommands.retainAll(serverCommands);

            // if no supported command found, we have to close the connection
            if (clientCommands.isEmpty()) {
                String dataMessage = "required command not supported by client";
                StringBuilder data = new StringBuilder();
                data.append("500 ").append(dataMessage).append("\n");

                writeCommand(e.getChannel(), "rsp", data.toString());
                e.getChannel().close();
                return;
            }

            StringBuilder commandBuffer = new StringBuilder();
            Iterator<String> commandIterator = clientCommands.iterator();
            while (commandIterator.hasNext()) {
                String command = commandIterator.next();
                commandBuffer.append(command);
                if (commandIterator.hasNext()) {
                    commandBuffer.append(",");
                }
            }

            String dataMessage = "OK";
            StringBuilder data = new StringBuilder();
            data.append("200 ").append(dataMessage).append("\n");
            data.append("relp_version=").append(inputProperties.get("relp_version")).append("\n");
            data.append("commands=").append(commandBuffer.toString());

            writeCommand(e.getChannel(), "rsp", data.toString());
            alreadyOpened = true;
        } else {
            if (clientCommand.equals("syslog")) {
                ChannelBuffer syslogBuffer = ChannelBuffers.copiedBuffer(clientData, charset);
                Channels.fireMessageReceived(ctx, syslogBuffer);
                writeCommand(e.getChannel(), "rsp", "200 OK");
            } else if (clientCommand.equals("close")) {
                writeCommand(e.getChannel(), "rsp", "");
            } else {
                e.getChannel().close();
                return;
            }
        }

        transactionNumber++;
    }
 
    @Override
    public void closeRequested(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        try {
            e.getChannel().write(ChannelBuffers.copiedBuffer("0 serverclose 0\n", charset));
        } catch (Exception ex) {
        } finally {
            super.closeRequested(ctx, e);
        }
    }

    private void writeCommand(Channel channel, String command, String data) throws UnsupportedEncodingException {
        StringBuilder response = new StringBuilder();
        response.append(transactionNumber).append(" ");
        response.append(command).append(" ");
        response.append(data.length());

        if (data.length() > 0) {
            response.append(" ").append(data);
        }

        response.append("\n");

        channel.write(ChannelBuffers.copiedBuffer(response.toString(), charset));
    }
}
