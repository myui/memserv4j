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

import memserv4j.util.lang.ArrayUtils;
import memserv4j.util.lang.Primitives;

import java.util.Arrays;

public final class MemcachedUtils {

    private MemcachedUtils() {}

    public static byte[] makeInternalValue(final byte[] value, final int flags) {
        final byte[] storedValue;
        if (flags != 0) {
            storedValue = new byte[value.length + 5];
            storedValue[0] = 1;
            Primitives.putInt(storedValue, 1, flags);
            System.arraycopy(value, 0, storedValue, 5, value.length);
        } else {
            storedValue = new byte[value.length + 1];
            storedValue[0] = 0;
            System.arraycopy(value, 0, storedValue, 1, value.length);
        }
        return storedValue;
    }

    public static byte[] makeInternalValue(final int flags, final int valueLength) {
        final byte[] b;
        if (flags == 0) {
            b = new byte[valueLength + 1];
            b[0] = 0;
        } else {
            b = new byte[valueLength + 5];
            b[0] = 1;
            Primitives.putInt(b, 1, flags);
        }
        return b;
    }

    public static int getFlags(final byte[] internalValue) {
        if (internalValue == null) {
            return 0;
        }
        if (internalValue.length < 5) {
            return 0;
        }
        if (internalValue[0] != 1) {
            return 0;
        }
        int flags = Primitives.getInt(internalValue, 1);
        return flags;
    }

    public static byte[] getValue(final byte[] internalValue) {
        if (internalValue == null) {
            return null;
        }
        if (internalValue.length > 0) {
            if (internalValue[0] == 0) {
                return ArrayUtils.copyOfRange(internalValue, 1, internalValue.length);
            } else if (internalValue[0] == 1) {
                int valuelen = internalValue.length - 5;
                if (valuelen > 0) {
                    return ArrayUtils.copyOfRange(internalValue, 5, internalValue.length);
                } else if (valuelen == 0) {
                    return new byte[0];
                }
            }
        }
        throw new IllegalArgumentException(
            "Unexpected memcached internal value: " + Arrays.toString(internalValue));
    }

}
