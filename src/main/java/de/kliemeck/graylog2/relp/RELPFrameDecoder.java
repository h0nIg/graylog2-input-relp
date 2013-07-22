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

import java.nio.charset.Charset;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.frame.FrameDecoder;

/**
 * @author Hans-Joachim Kliemeck <git@kliemeck.de>
 */
public class RELPFrameDecoder extends FrameDecoder {
    private static final String frameStructure = 
        "^"
        + "\\d{1,9}"
        + "( "
            + "([a-zA-Z]{1,32}"
                + "( "
                    + "(\\d{1,9}"
                        + "("
                            + "( "
                                + "(.+)?"
                            + ")?"
                        + ")?"
                    + "|"
                        + "0\n?"
                    + ")?"
                + ")?"
            + ")?"
        + ")?";
    private static final Pattern framePattern = Pattern.compile(frameStructure, Pattern.DOTALL);
    private static final Pattern dataLengthPattern = Pattern.compile("^(\\d{1,9}).+$", Pattern.DOTALL);
    private static final Pattern frameHeaderPattern = Pattern.compile("^(\\d{1,9} [a-zA-Z]{1,32} \\d{1,9})(.+)$", Pattern.DOTALL);

    @Override
    protected Object decode(ChannelHandlerContext ctx, Channel channel, ChannelBuffer buffer) throws Exception {
        String input = buffer.toString(Charset.forName("UTF-8"));
        Matcher matcher = framePattern.matcher(input);
        if (!matcher.matches()) {
            // framing error
            channel.close();
            return null;
        }

        int dataLength;

        try {
            // empty data
            String data = matcher.group(6);
            if (data == null) {
                dataLength = 0;
            } else {
                String dataWithLength = matcher.group(4);

                Matcher dataLengthMatcher = dataLengthPattern.matcher(dataWithLength);
                dataLengthMatcher.matches();

                // data length inclusive previous whitespace
                dataLength = Integer.parseInt(dataLengthMatcher.group(1)) + 1;
                if (data.length() < dataLength) {
                    return null;
                }
            }
        } catch (IllegalStateException e) {
            // group does not exists, not enougth data available
            return null;
        }

        // if more than 1 message arrived, we have to split them
        Matcher headerMatcher = frameHeaderPattern.matcher(input);
        headerMatcher.matches();

        String header = headerMatcher.group(1);
        String data = headerMatcher.group(2);

        // skip the extracted frame inclusive the separator at the end
        buffer.skipBytes(header.length() + dataLength + 1);

        // return the header inclusive the whitespace between header and data without the last \n on data.
        // the last \n is not added because substring end index is exclusive
        return header + data.substring(0, dataLength);
    }
}