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
package memserv4j.util.net;

import memserv4j.Settings;
import memserv4j.util.StringUtils;
import memserv4j.util.SystemUtils;

import java.io.IOException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.channels.SocketChannel;
import java.util.Enumeration;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public final class NetUtils {

    private static final String BIND_NIC;
    private static final int MAC_ADDRESS_TOKENS = 6;
    static {
        BIND_NIC = Settings.getThroughSystemProperty("memserv4j.net.bind_interface");
    }

    private NetUtils() {}

    /**
     * Get local address without loopback address.
     * 
     * @throws
     */
    public static InetAddress getLocalHost() {
        final InetAddress addr = getLocalHost(false);
        if (addr == null) {
            throw new IllegalStateException("No valid IP address for this host found");
        }
        return addr;
    }

    /**
     * @return null if no valid address found.
     */
    public static InetAddress getLocalHost(boolean allowLoopbackAddr) {
        if (BIND_NIC != null) {
            final NetworkInterface nic;
            try {
                nic = NetworkInterface.getByName(BIND_NIC);
            } catch (SocketException e) {
                throw new IllegalStateException("Error while getting NetworkInterface: " + BIND_NIC,
                    e);
            }
            if (nic == null) {
                final StringBuilder buf = new StringBuilder(128);
                buf.append("{ ");
                try {
                    Enumeration<NetworkInterface> nics = NetworkInterface.getNetworkInterfaces();
                    boolean hasMore = nics.hasMoreElements();
                    while (hasMore) {
                        NetworkInterface n = nics.nextElement();
                        String nicName = n.getName();
                        buf.append(nicName);
                        if ((hasMore = nics.hasMoreElements()) == true) {
                            buf.append(',');
                        }
                    }
                } catch (SocketException se) {
                    ;
                }
                buf.append(" }");
                throw new IllegalArgumentException("NIC '" + BIND_NIC + "' not found in " + buf);
            }
            final Enumeration<InetAddress> nicAddrs = nic.getInetAddresses();
            while (nicAddrs.hasMoreElements()) {
                final InetAddress nicAddr = nicAddrs.nextElement();
                if (!nicAddr.isLoopbackAddress()/* && !nicAddr.isLinkLocalAddress() */) {
                    return nicAddr;
                }
            }
            return null;
        }
        InetAddress localHost = null;
        try {
            InetAddress probeAddr = InetAddress.getLocalHost();
            if (allowLoopbackAddr) {
                localHost = probeAddr;
            }
            if (probeAddr.isLoopbackAddress()/* || probeAddr.isLinkLocalAddress() */) {
                final Enumeration<NetworkInterface> nics = NetworkInterface.getNetworkInterfaces();
                nicLoop: while (nics.hasMoreElements()) {
                    NetworkInterface nic = nics.nextElement();
                    if (nic.isLoopback()) {
                        continue;
                    }
                    final Enumeration<InetAddress> nicAddrs = nic.getInetAddresses();
                    while (nicAddrs.hasMoreElements()) {
                        InetAddress nicAddr = nicAddrs.nextElement();
                        if (!nicAddr.isLoopbackAddress()/* && !nicAddr.isLinkLocalAddress() */) {
                            localHost = nicAddr;
                            if (nic.isVirtual()) {
                                continue nicLoop; // try to find IP-address of non-virtual NIC
                            } else {
                                break nicLoop;
                            }
                        }
                    }
                }
            } else {
                localHost = probeAddr;
            }
        } catch (UnknownHostException ue) {
            throw new IllegalStateException(ue);
        } catch (SocketException se) {
            throw new IllegalStateException(se);
        }
        return localHost;
    }

    public static String getLocalHostName() {
        return getLocalHost().getHostName();
    }

    public static String getLocalHostAddress() {
        return getLocalHost().getHostAddress();
    }

    /**
     * @link http://www.ietf.org/rfc/rfc2396.txt
     */
    public static String getLocalHostAddressAsUrlString() {
        final InetAddress addr = getLocalHost();
        final String hostaddr = addr.getHostAddress();
        if (isIpV6Address(addr)) {
            // hostaddr = hostaddr.replaceAll("%", "%25")
            String v6addr = '[' + hostaddr + ']';
            return v6addr;
        }
        return hostaddr;
    }

    public static boolean isIpV6Address(final InetAddress addr) {
        return addr instanceof Inet6Address;
    }

    public static String getHostNameWithoutDomain(final InetAddress addr) {
        final String hostName = addr.getHostName();
        final int pos = hostName.indexOf('.');
        if (pos == -1) {
            return hostName;
        } else {
            return hostName.substring(0, pos);
        }
    }

    public static String getFQDN(final InetAddress addr) {
        String hostname = addr.getHostName();
        if (hostname.indexOf('.') >= 0) {
            return hostname;
        }
        hostname = addr.getCanonicalHostName();
        if (hostname.indexOf('.') >= 0) {
            return hostname;
        }
        String hostAddr = addr.getHostAddress();
        try {
            return InetAddress.getByName(hostAddr).getHostName();
        } catch (UnknownHostException e) {
            return hostAddr;
        }
    }

    public static int getAvailablePort() {
        try {
            ServerSocket s = new ServerSocket(0);
            s.setReuseAddress(true);
            s.close();
            return s.getLocalPort();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to find an available port", e);
        }
    }

    public static int getAvialablePort(final int basePort) {
        if (basePort == 0) {
            return getAvailablePort();
        }
        if (basePort < 0 || basePort > 65535) {
            throw new IllegalArgumentException("Illegal port number: " + basePort);
        }
        for (int i = basePort; i <= 65535; i++) {
            if (isPortAvailable(i)) {
                return i;
            }
        }
        throw new NoSuchElementException(
            "Could not find available port greater than or equals to " + basePort);
    }

    public static boolean isPortAvailable(final int port) {
        ServerSocket s = null;
        try {
            s = new ServerSocket(port);
            s.setReuseAddress(true);
            return true;
        } catch (IOException e) {
            return false;
        } finally {
            if (s != null) {
                try {
                    s.close();
                } catch (IOException e) {
                    ;
                }
            }
        }
    }

    public static InetSocketAddress getAnyLocalInetSocketAddress() {
        InetAddress addr = getLocalHost(false);
        int port = getAvailablePort();
        return new InetSocketAddress(addr, port);
    }

    public static URI toURI(final URL url) {
        try {
            return url.toURI();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static void closeQuietly(final Socket socket) {
        try {
            socket.close();
        } catch (IOException e) {
            ;
        }
    }

    public static void closeQuietly(final SocketChannel channel) {
        final Socket socket = channel.socket();
        try {
            socket.close();
        } catch (IOException e) {
            ;
        }
        try {
            channel.close();
        } catch (IOException e) {
            ;
        }
    }

    public static void shutdownAndCloseQuietly(final Socket socket) {
        try {
            socket.shutdownOutput();
        } catch (IOException e) {
            ;
        }
        try {
            socket.close();
        } catch (IOException e) {
            ;
        }
    }

    public static void shutdownOutputQuietly(final Socket sock) {
        try {
            sock.shutdownOutput();
        } catch (IOException e) {
            ;
        }
    }

    /**
     * Usually returns MAC address (48 bits = 6 bytes).
     */
    public static byte[] getMacAddress(final InetAddress addr) {
        if (SystemUtils.getJavaVersion() < 1.6f) {
            return null; // getHardwareAddress is not supported
        }
        final NetworkInterface ni;
        try {
            ni = NetworkInterface.getByInetAddress(addr);
        } catch (SocketException e) {
            return null;
        }
        if (ni != null) {
            final byte[] mac;
            try {
                mac = ni.getHardwareAddress();
            } catch (SocketException e) {
                return null;
            }
            return mac;
        }
        return null;
    }

    public static String getMacAddressStr(final InetAddress addr) {
        final byte[] mac = getMacAddress(addr);
        if (mac == null) {
            return null;
        }
        return encodeMacAddress(mac);
    }

    /**
     * Extract each array of mac address and convert it to hexa with the following format
     * 08-00-27-DC-4A-9E.
     */
    public static String encodeMacAddress(final byte[] mac) {
        final StringBuilder buf = new StringBuilder(20);
        final int macLength = mac.length;
        final int last = macLength - 1;
        for (int i = 0; i < macLength; i++) {
            String s = String.format("%02X%s", mac[i], (i == last) ? "" : "-");
            buf.append(s);
        }
        return buf.toString();
    }

    public static byte[] decodeMacAddress(final String mac) {
        final StringTokenizer tokens = new StringTokenizer(mac, "-");
        if (tokens.countTokens() != MAC_ADDRESS_TOKENS) {
            throw new IllegalArgumentException("Unexpected mac address representation: " + mac);
        }
        final StringBuilder buf = new StringBuilder(MAC_ADDRESS_TOKENS * 2);
        for (int i = 0; i < MAC_ADDRESS_TOKENS; i++) {
            buf.append(tokens.nextToken());
        }
        char[] c = buf.toString().toCharArray();
        return StringUtils.decodeHex(c);
    }

    @Nullable
    public static InetAddress getInetAddressByName(final String host) {
        try {
            return InetAddress.getByName(host);
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException("Cannot find InetAddress for host: " + host);
        }
    }

    @Nonnull
    public static InetAddress getInetAddress(final String addressOrName) {
        if (isIPAddress(addressOrName)) {
            return getInetAddress(addressOrName);
        } else {
            return getInetAddressByName(addressOrName);
        }
    }

    public static boolean isIPAddress(final String ip) {
        return ip.matches(
            "^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$");
    }
}
