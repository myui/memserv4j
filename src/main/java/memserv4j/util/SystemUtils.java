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
package memserv4j.util;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.Method;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.List;

import org.apache.commons.logging.LogFactory;

public final class SystemUtils {

    public static final String OS_NAME = System.getProperty("os.name");
    public static final String JAVA_VERSION = System.getProperty("java.version");
    public static final boolean IS_OS_SUN_OS =
            OS_NAME.startsWith("SunOS") || OS_NAME.startsWith("Solaris");
    public static final boolean IS_OS_LINUX =
            OS_NAME.startsWith("Linux") || OS_NAME.startsWith("LINUX");
    public static final boolean IS_OS_WINDOWS = OS_NAME.startsWith("Windows");
    public static final float JAVA_VERSION_FLOAT = parseJavaVersion(getJavaVersion(JAVA_VERSION));

    private static final int NPROCS = Runtime.getRuntime().availableProcessors();
    public static final boolean IS_SUN_VM =
            System.getProperty("java.vm.vendor").indexOf("Sun") != -1;

    private static final boolean preferSigar;
    private static Object sigarInstance = null;
    private static Method sigarCpuPercMtd = null;
    private static Method sigarCpuCombinedMtd = null;
    private static Method sigarIoWaitMtd = null;
    private static final MemoryMXBean mbean = ManagementFactory.getMemoryMXBean();
    static {
        preferSigar = initializeSigar();
    }

    private SystemUtils() {}

    private static boolean initializeSigar() {
        try {
            Object sigar = Class.forName("org.hyperic.sigar.Sigar").newInstance();
            Method proxyMtd = Class.forName("org.hyperic.sigar.SigarProxyCache")
                                   .getMethod("newInstance", sigar.getClass(), int.class);
            // Update caches every 2 seconds.
            sigarInstance = proxyMtd.invoke(null, sigar, 2000);
            sigarCpuPercMtd = sigarInstance.getClass().getMethod("getCpuPerc");
            sigarCpuCombinedMtd = sigarCpuPercMtd.getReturnType().getMethod("getCombined");
            sigarIoWaitMtd = sigarCpuPercMtd.getReturnType().getMethod("getWait");
            return true;
        } catch (Exception e) {
            LogFactory.getLog(SystemUtils.class).error("Failed to initilize Hyperic Sigar", e);
            return false;
        }
    }

    public static float getJavaVersion() {
        return JAVA_VERSION_FLOAT;
    }

    private static float parseJavaVersion(String versionStr) {
        if (versionStr == null) {
            throw new IllegalArgumentException();
        }
        // e.g., 1.3.12, 1.6
        String str = versionStr.substring(0, 3);
        if (versionStr.length() >= 5) {
            str = str + versionStr.substring(4, 5);
        }
        return Float.parseFloat(str);
    }

    private static String getJavaVersion(String versionStr) {
        for (int i = 0; i < versionStr.length(); i++) {
            char c = versionStr.charAt(i);
            if (Character.isDigit(c)) {
                return versionStr.substring(i);
            }
        }
        return null;
    }

    public static boolean isSunVM() {
        return IS_SUN_VM;
    }

    public static boolean is64BitVM() {
        try {
            int bits = Integer.getInteger("sun.arch.data.model", 0).intValue();
            if (bits != 0) {
                return bits == 64;
            }
            return System.getProperty("java.vm.name").indexOf("64") >= 0;
        } catch (Throwable t) {
            return false;
        }
    }

    public static boolean isEpollEnabled() {
        final String osname = AccessController.doPrivileged(new GetPropertyAction("os.name"));
        if ("SunOS".equals(osname)) {
            return true;
        }

        // use EPollSelectorProvider for Linux kernels >= 2.6
        if ("Linux".equals(osname)) {
            String osversion = AccessController.doPrivileged(new GetPropertyAction("os.version"));
            final String[] vers = osversion.split("\\.", 0);
            if (vers.length >= 2) {
                try {
                    final int major = Integer.parseInt(vers[0]);
                    final int minor = Integer.parseInt(vers[1]);
                    if (major > 2 || (major == 2 && minor >= 6)) {
                        return true;
                    }
                } catch (NumberFormatException x) {
                    // format not recognized
                }
            }
        }
        return false;
    }

    public static final class GetPropertyAction implements PrivilegedAction<String> {

        private final String name;
        private final String defaultValue;

        public GetPropertyAction(String name, String defaultValue) {
            this.name = name;
            this.defaultValue = defaultValue;
        }

        public GetPropertyAction(String name) {
            this(name, null);
        }

        public String run() {
            return System.getProperty(name, defaultValue);
        }

    }

    public static int availableProcessors() {
        return NPROCS;
    }

    public static boolean isSendfileSupported() {
        return IS_OS_SUN_OS || (IS_OS_LINUX && getJavaVersion() >= 1.7f);
    }

    public static boolean isMunmapAvailable() {
        return !IS_OS_WINDOWS || getJavaVersion() >= 1.7f;
    }

    public static long getFreeMemory() {
        final Runtime runtime = Runtime.getRuntime();
        return runtime.freeMemory();
    }

    public static long getHeapFreeMemory() {
        MemoryUsage usage = mbean.getHeapMemoryUsage();
        final long max = usage.getMax();
        final long used = usage.getUsed();
        final long free = max - used;
        return free;
    }

    public static long getHeapUsedMemory() {
        MemoryUsage usage = mbean.getHeapMemoryUsage();
        final long used = usage.getUsed();
        return used;
    }

    public static float getHeapFreeRatio() {
        MemoryUsage usage = mbean.getHeapMemoryUsage();
        final long max = usage.getMax();
        final long used = usage.getUsed();
        final long free = max - used;
        return (float) free / (float) max;
    }

    public static int getHeapFreePercentage() {
        return (int) (getHeapFreeRatio() * 100f);
    }

    public static int countGC() {
        int count = 0;
        final List<GarbageCollectorMXBean> gclist = ManagementFactory.getGarbageCollectorMXBeans();
        for (GarbageCollectorMXBean gcmx : gclist) {
            count += gcmx.getCollectionCount();
        }
        return count;
    }

    /**
     * @return uptime in msec
     */
    public static long getUptime() {
        final RuntimeMXBean mx = ManagementFactory.getRuntimeMXBean();
        return mx.getUptime();
    }

    public static double getCpuLoadAverage() {
        if (preferSigar) {
            final Double cpuload;
            try {
                cpuload =
                        (Double) sigarCpuCombinedMtd.invoke(sigarCpuPercMtd.invoke(sigarInstance));
            } catch (Exception e) {
                LogFactory.getLog(SystemUtils.class)
                          .error("Failed to obtain CPU load via Hyperic Sigar", e);
                return -1d;
            }
            return cpuload.doubleValue();
        } else {
            OperatingSystemMXBean mx = ManagementFactory.getOperatingSystemMXBean();
            com.sun.management.OperatingSystemMXBean sunmx =
                    (com.sun.management.OperatingSystemMXBean) mx;
            double d = sunmx.getSystemLoadAverage();
            if (d > 0) {
                return d / NPROCS;
            }
            return d;
        }
    }

    public static double getIoWait() {
        if (preferSigar) {
            final Double iowait;
            try {
                iowait = (Double) sigarIoWaitMtd.invoke(sigarCpuPercMtd.invoke(sigarInstance));
            } catch (Exception e) {
                LogFactory.getLog(SystemUtils.class)
                          .error("Failed to obtain CPU iowait via Hyperic Sigar", e);
                return -1d;
            }
            return iowait.doubleValue();
        } else {
            return -1d;
        }
    }

    /** return in nano-seconds */
    @Deprecated
    public static long getProcessCpuTime() {
        if (preferSigar) {
            throw new UnsupportedOperationException(
                "SystemUtils#getProcessCpuTime() is not supported when using Hyperic Sigar");
        } else {
            OperatingSystemMXBean mx = ManagementFactory.getOperatingSystemMXBean();
            com.sun.management.OperatingSystemMXBean sunmx =
                    (com.sun.management.OperatingSystemMXBean) mx;
            return sunmx.getProcessCpuTime();
        }
    }

    public static CPUInfo getCPUInfo(CPUInfo prev) {
        final long cpuTime = getProcessCpuTime();
        final long upTime = getUptime();
        final long elapsedCpu = cpuTime - prev.cpuTime;
        long elapsedUp = upTime - prev.upTime;
        if (elapsedUp == 0L) {
            elapsedUp = 1L;
        }
        // nano (10^6) * percent (100) = 10000F
        final float cpuUsage = Math.min(99F, elapsedCpu / (elapsedUp * 10000F * NPROCS));
        return new CPUInfo(cpuTime, upTime, cpuUsage);
    }

    public static final class CPUInfo {

        final long cpuTime;
        final long upTime;
        final float cpuLoad;

        public CPUInfo() {
            this.cpuTime = getProcessCpuTime();
            this.upTime = getUptime();
            this.cpuLoad = 1L;
        }

        public CPUInfo(long procCpuTime, long upTime, float cpuUsage) {
            this.cpuTime = procCpuTime;
            this.upTime = upTime;
            this.cpuLoad = cpuUsage;
        }

        public float getCpuLoad() {
            return cpuLoad;
        }
    }

}
