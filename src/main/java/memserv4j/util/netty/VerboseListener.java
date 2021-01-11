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
package memserv4j.util.netty;

import javax.annotation.Nonnull;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;

public final class VerboseListener implements ChannelFutureListener {
    private static final Log LOG = LogFactory.getLog(VerboseListener.class);

    public static final VerboseListener VERBOSE = new VerboseListener("operation");

    @Nonnull
    private final String name;

    public VerboseListener(@Nonnull String name) {
        this.name = name;
    }

    @Override
    public void operationComplete(ChannelFuture f) throws Exception {
        assert f.isDone();
        if (f.isCancelled()) {
            LOG.error(name + " is cancelled", f.getCause());
        } else if (!f.isSuccess()) {
            LOG.error(name + " is failed", f.getCause());
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug(name + " is succeeded");
            }
        }
    }

}
