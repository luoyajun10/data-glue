package com.data.integration.sink.hbase.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.security.SecureRandom;

public class BytesUtil {

    private static final String UTF8_ENCODING = "UTF-8";
    private static final Charset UTF8_CHARSET = Charset.forName("UTF-8");
    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    public static final int SIZEOF_BOOLEAN = 1;
    public static final int SIZEOF_BYTE = 1;
    public static final int SIZEOF_CHAR = 2;
    public static final int SIZEOF_DOUBLE = 8;
    public static final int SIZEOF_FLOAT = 4;
    public static final int SIZEOF_INT = 4;
    public static final int SIZEOF_LONG = 8;
    public static final int SIZEOF_SHORT = 2;
    public static final int ESTIMATED_HEAP_TAX = 16;
    private static final SecureRandom RNG = new SecureRandom();

    public BytesUtil() {
    }

    public static final int len(byte[] b) {
        return b == null ? 0 : b.length;
    }

    public static int putBytes(byte[] tgtBytes, int tgtOffset, byte[] srcBytes, int srcOffset, int srcLength) {
        System.arraycopy(srcBytes, srcOffset, tgtBytes, tgtOffset, srcLength);
        return tgtOffset + srcLength;
    }

    public static int putByte(byte[] bytes, int offset, byte b) {
        bytes[offset] = b;
        return offset + 1;
    }

    public static int putByteBuffer(byte[] bytes, int offset, ByteBuffer buf) {
        int len = buf.remaining();
        buf.get(bytes, offset, len);
        return offset + len;
    }

    public static byte[] toBytes(ByteBuffer buf) {
        ByteBuffer dup = buf.duplicate();
        dup.position(0);
        return readBytes(dup);
    }

    private static byte[] readBytes(ByteBuffer buf) {
        byte[] result = new byte[buf.remaining()];
        buf.get(result);
        return result;
    }

    public static String toString(byte[] b) {
        return b == null ? null : toString(b, 0, b.length);
    }

    public static String toString(byte[] b1, String sep, byte[] b2) {
        return toString(b1, 0, b1.length) + sep + toString(b2, 0, b2.length);
    }

    public static String toString(byte[] b, int off) {
        if (b == null) {
            return null;
        } else {
            int len = b.length - off;
            return len <= 0 ? "" : new String(b, off, len, UTF8_CHARSET);
        }
    }

    public static String toString(byte[] b, int off, int len) {
        if (b == null) {
            return null;
        } else {
            return len == 0 ? "" : new String(b, off, len, UTF8_CHARSET);
        }
    }

    public static String toStringBinary(byte[] b) {
        return b == null ? "null" : toStringBinary(b, 0, b.length);
    }

    public static String toStringBinary(ByteBuffer buf) {
        if (buf == null) {
            return "null";
        } else {
            return buf.hasArray() ? toStringBinary(buf.array(), buf.arrayOffset(), buf.limit()) : toStringBinary(toBytes(buf));
        }
    }

    public static String toStringBinary(byte[] b, int off, int len) {
        StringBuilder result = new StringBuilder();
        if (off >= b.length) {
            return result.toString();
        } else {
            if (off + len > b.length) {
                len = b.length - off;
            }

            for(int i = off; i < off + len; ++i) {
                int ch = b[i] & 255;
                if ((ch < 48 || ch > 57) && (ch < 65 || ch > 90) && (ch < 97 || ch > 122) && " `~!@#$%^&*()-_=+[]{}|;:'\",.<>/?".indexOf(ch) < 0) {
                    result.append(String.format("\\x%02X", ch));
                } else {
                    result.append((char)ch);
                }
            }

            return result.toString();
        }
    }

    private static boolean isHexDigit(char c) {
        return c >= 'A' && c <= 'F' || c >= '0' && c <= '9';
    }

    public static byte toBinaryFromHex(byte ch) {
        return ch >= 65 && ch <= 70 ? (byte)(10 + (byte)(ch - 65)) : (byte)(ch - 48);
    }

    public static byte[] toBytesBinary(String in) {
        byte[] b = new byte[in.length()];
        int size = 0;

        for(int i = 0; i < in.length(); ++i) {
            char ch = in.charAt(i);
            if (ch == '\\' && in.length() > i + 1 && in.charAt(i + 1) == 'x') {
                char hd1 = in.charAt(i + 2);
                char hd2 = in.charAt(i + 3);
                if (isHexDigit(hd1) && isHexDigit(hd2)) {
                    byte d = (byte)((toBinaryFromHex((byte)hd1) << 4) + toBinaryFromHex((byte)hd2));
                    b[size++] = d;
                    i += 3;
                }
            } else {
                b[size++] = (byte)ch;
            }
        }

        byte[] b2 = new byte[size];
        System.arraycopy(b, 0, b2, 0, size);
        return b2;
    }

    public static byte[] toBytes(String s) {
        return s.getBytes(UTF8_CHARSET);
    }

    public static byte[] toBytes(boolean b) {
        return new byte[]{(byte)(b ? -1 : 0)};
    }

    public static boolean toBoolean(byte[] b) {
        if (b.length != 1) {
            throw new IllegalArgumentException("Array has wrong size: " + b.length);
        } else {
            return b[0] != 0;
        }
    }

    public static byte[] toBytes(long val) {
        byte[] b = new byte[8];

        for(int i = 7; i > 0; --i) {
            b[i] = (byte)((int)val);
            val >>>= 8;
        }

        b[0] = (byte)((int)val);
        return b;
    }

    public static long toLong(byte[] bytes) {
        return toLong(bytes, 0, 8);
    }

    public static long toLong(byte[] bytes, int offset) {
        return toLong(bytes, offset, 8);
    }

    public static long toLong(byte[] bytes, int offset, int length) {
        if (length == 8 && offset + length <= bytes.length) {
            long l = 0L;

            for(int i = offset; i < offset + length; ++i) {
                l <<= 8;
                l ^= (long)(bytes[i] & 255);
            }

            return l;
        } else {
            throw explainWrongLengthOrOffset(bytes, offset, length, 8);
        }
    }

    private static IllegalArgumentException explainWrongLengthOrOffset(byte[] bytes, int offset, int length, int expectedLength) {
        String reason;
        if (length != expectedLength) {
            reason = "Wrong length: " + length + ", expected " + expectedLength;
        } else {
            reason = "offset (" + offset + ") + length (" + length + ") exceed the" + " capacity of the array: " + bytes.length;
        }

        return new IllegalArgumentException(reason);
    }

    public static int putLong(byte[] bytes, int offset, long val) {
        if (bytes.length - offset < 8) {
            throw new IllegalArgumentException("Not enough room to put a long at offset " + offset + " in a " + bytes.length + " byte array");
        } else {
            for(int i = offset + 7; i > offset; --i) {
                bytes[i] = (byte)((int)val);
                val >>>= 8;
            }

            bytes[offset] = (byte)((int)val);
            return offset + 8;
        }
    }

    public static float toFloat(byte[] bytes) {
        return toFloat(bytes, 0);
    }

    public static float toFloat(byte[] bytes, int offset) {
        return Float.intBitsToFloat(toInt(bytes, offset, 4));
    }

    public static int putFloat(byte[] bytes, int offset, float f) {
        return putInt(bytes, offset, Float.floatToRawIntBits(f));
    }

    public static byte[] toBytes(float f) {
        return toBytes(Float.floatToRawIntBits(f));
    }

    public static double toDouble(byte[] bytes) {
        return toDouble(bytes, 0);
    }

    public static double toDouble(byte[] bytes, int offset) {
        return Double.longBitsToDouble(toLong(bytes, offset, 8));
    }

    public static int putDouble(byte[] bytes, int offset, double d) {
        return putLong(bytes, offset, Double.doubleToLongBits(d));
    }

    public static byte[] toBytes(double d) {
        return toBytes(Double.doubleToRawLongBits(d));
    }

    public static byte[] toBytes(int val) {
        byte[] b = new byte[4];

        for(int i = 3; i > 0; --i) {
            b[i] = (byte)val;
            val >>>= 8;
        }

        b[0] = (byte)val;
        return b;
    }

    public static int toInt(byte[] bytes) {
        return toInt(bytes, 0, 4);
    }

    public static int toInt(byte[] bytes, int offset) {
        return toInt(bytes, offset, 4);
    }

    public static int toInt(byte[] bytes, int offset, int length) {
        if (length == 4 && offset + length <= bytes.length) {
            int n = 0;

            for(int i = offset; i < offset + length; ++i) {
                n <<= 8;
                n ^= bytes[i] & 255;
            }

            return n;
        } else {
            throw explainWrongLengthOrOffset(bytes, offset, length, 4);
        }
    }

    public static int readAsInt(byte[] bytes, int offset, int length) {
        if (offset + length > bytes.length) {
            throw new IllegalArgumentException("offset (" + offset + ") + length (" + length + ") exceed the" + " capacity of the array: " + bytes.length);
        } else {
            int n = 0;

            for(int i = offset; i < offset + length; ++i) {
                n <<= 8;
                n ^= bytes[i] & 255;
            }

            return n;
        }
    }

    public static int putInt(byte[] bytes, int offset, int val) {
        if (bytes.length - offset < 4) {
            throw new IllegalArgumentException("Not enough room to put an int at offset " + offset + " in a " + bytes.length + " byte array");
        } else {
            for(int i = offset + 3; i > offset; --i) {
                bytes[i] = (byte)val;
                val >>>= 8;
            }

            bytes[offset] = (byte)val;
            return offset + 4;
        }
    }

    public static byte[] toBytes(short val) {
        byte[] b = new byte[]{0, (byte)val};
        val = (short)(val >> 8);
        b[0] = (byte)val;
        return b;
    }

    public static short toShort(byte[] bytes) {
        return toShort(bytes, 0, 2);
    }

    public static short toShort(byte[] bytes, int offset) {
        return toShort(bytes, offset, 2);
    }

    public static short toShort(byte[] bytes, int offset, int length) {
        if (length == 2 && offset + length <= bytes.length) {
            short n = 0;
            n = (short)(n ^ bytes[offset] & 255);
            n = (short)(n << 8);
            n = (short)(n ^ bytes[offset + 1] & 255);
            return n;
        } else {
            throw explainWrongLengthOrOffset(bytes, offset, length, 2);
        }
    }

    public static byte[] getBytes(ByteBuffer buf) {
        return readBytes(buf.duplicate());
    }

    public static int putShort(byte[] bytes, int offset, short val) {
        if (bytes.length - offset < 2) {
            throw new IllegalArgumentException("Not enough room to put a short at offset " + offset + " in a " + bytes.length + " byte array");
        } else {
            bytes[offset + 1] = (byte)val;
            val = (short)(val >> 8);
            bytes[offset] = (byte)val;
            return offset + 2;
        }
    }

    public static int putAsShort(byte[] bytes, int offset, int val) {
        if (bytes.length - offset < 2) {
            throw new IllegalArgumentException("Not enough room to put a short at offset " + offset + " in a " + bytes.length + " byte array");
        } else {
            bytes[offset + 1] = (byte)val;
            val >>= 8;
            bytes[offset] = (byte)val;
            return offset + 2;
        }
    }

    public static byte[] toBytes(BigDecimal val) {
        byte[] valueBytes = val.unscaledValue().toByteArray();
        byte[] result = new byte[valueBytes.length + 4];
        int offset = putInt(result, 0, val.scale());
        putBytes(result, offset, valueBytes, 0, valueBytes.length);
        return result;
    }

    public static BigDecimal toBigDecimal(byte[] bytes) {
        return toBigDecimal(bytes, 0, bytes.length);
    }

    public static BigDecimal toBigDecimal(byte[] bytes, int offset, int length) {
        if (bytes != null && length >= 5 && offset + length <= bytes.length) {
            int scale = toInt(bytes, offset);
            byte[] tcBytes = new byte[length - 4];
            System.arraycopy(bytes, offset + 4, tcBytes, 0, length - 4);
            return new BigDecimal(new BigInteger(tcBytes), scale);
        } else {
            return null;
        }
    }

    public static int putBigDecimal(byte[] bytes, int offset, BigDecimal val) {
        if (bytes == null) {
            return offset;
        } else {
            byte[] valueBytes = val.unscaledValue().toByteArray();
            byte[] result = new byte[valueBytes.length + 4];
            offset = putInt(result, offset, val.scale());
            return putBytes(result, offset, valueBytes, 0, valueBytes.length);
        }
    }

    public static boolean equals(byte[] a, ByteBuffer buf) {
        if (a == null) {
            return buf == null;
        } else if (buf == null) {
            return false;
        } else if (a.length != buf.remaining()) {
            return false;
        } else {
            ByteBuffer b = buf.duplicate();
            byte[] arr$ = a;
            int len$ = a.length;

            for(int i$ = 0; i$ < len$; ++i$) {
                byte anA = arr$[i$];
                if (anA != b.get()) {
                    return false;
                }
            }

            return true;
        }
    }

    public static byte[] add(byte[] a, byte[] b) {
        return add(a, b, EMPTY_BYTE_ARRAY);
    }

    public static byte[] add(byte[] a, byte[] b, byte[] c) {
        byte[] result = new byte[a.length + b.length + c.length];
        System.arraycopy(a, 0, result, 0, a.length);
        System.arraycopy(b, 0, result, a.length, b.length);
        System.arraycopy(c, 0, result, a.length + b.length, c.length);
        return result;
    }

    public static byte[] head(byte[] a, int length) {
        if (a.length < length) {
            return null;
        } else {
            byte[] result = new byte[length];
            System.arraycopy(a, 0, result, 0, length);
            return result;
        }
    }

    public static byte[] tail(byte[] a, int length) {
        if (a.length < length) {
            return null;
        } else {
            byte[] result = new byte[length];
            System.arraycopy(a, a.length - length, result, 0, length);
            return result;
        }
    }

    public static byte[] padHead(byte[] a, int length) {
        byte[] padding = new byte[length];

        for(int i = 0; i < length; ++i) {
            padding[i] = 0;
        }

        return add(padding, a);
    }

    public static byte[] padTail(byte[] a, int length) {
        byte[] padding = new byte[length];

        for(int i = 0; i < length; ++i) {
            padding[i] = 0;
        }

        return add(a, padding);
    }

    public static int hashCode(byte[] bytes, int offset, int length) {
        int hash = 1;

        for(int i = offset; i < offset + length; ++i) {
            hash = 31 * hash + bytes[i];
        }

        return hash;
    }

    public static byte[][] toByteArrays(String[] t) {
        byte[][] result = new byte[t.length][];

        for(int i = 0; i < t.length; ++i) {
            result[i] = toBytes(t[i]);
        }

        return result;
    }

    public static byte[][] toBinaryByteArrays(String[] t) {
        byte[][] result = new byte[t.length][];

        for(int i = 0; i < t.length; ++i) {
            result[i] = toBytesBinary(t[i]);
        }

        return result;
    }

    public static byte[][] toByteArrays(String column) {
        return toByteArrays(toBytes(column));
    }

    public static byte[][] toByteArrays(byte[] column) {
        byte[][] result = new byte[][]{column};
        return result;
    }

    public static byte[] incrementBytes(byte[] value, long amount) {
        byte[] val = value;
        if (value.length < 8) {
            byte[] newvalue;
            if (value[0] < 0) {
                newvalue = new byte[]{-1, -1, -1, -1, -1, -1, -1, -1};
            } else {
                newvalue = new byte[8];
            }

            System.arraycopy(value, 0, newvalue, newvalue.length - value.length, value.length);
            val = newvalue;
        } else if (value.length > 8) {
            throw new IllegalArgumentException("Increment Bytes - value too big: " + value.length);
        }

        if (amount == 0L) {
            return val;
        } else {
            return val[0] < 0 ? binaryIncrementNeg(val, amount) : binaryIncrementPos(val, amount);
        }
    }

    private static byte[] binaryIncrementPos(byte[] value, long amount) {
        long amo = amount;
        int sign = 1;
        if (amount < 0L) {
            amo = -amount;
            sign = -1;
        }

        for(int i = 0; i < value.length; ++i) {
            int cur = (int)amo % 256 * sign;
            amo >>= 8;
            int val = value[value.length - i - 1] & 255;
            int total = val + cur;
            if (total > 255) {
                amo += (long)sign;
                total %= 256;
            } else if (total < 0) {
                amo -= (long)sign;
            }

            value[value.length - i - 1] = (byte)total;
            if (amo == 0L) {
                return value;
            }
        }

        return value;
    }

    private static byte[] binaryIncrementNeg(byte[] value, long amount) {
        long amo = amount;
        int sign = 1;
        if (amount < 0L) {
            amo = -amount;
            sign = -1;
        }

        for(int i = 0; i < value.length; ++i) {
            int cur = (int)amo % 256 * sign;
            amo >>= 8;
            int val = (~value[value.length - i - 1] & 255) + 1;
            int total = cur - val;
            if (total >= 0) {
                amo += (long)sign;
            } else if (total < -256) {
                amo -= (long)sign;
                total %= 256;
            }

            value[value.length - i - 1] = (byte)total;
            if (amo == 0L) {
                return value;
            }
        }

        return value;
    }

    public static void writeStringFixedSize(DataOutput out, String s, int size) throws IOException {
        byte[] b = toBytes(s);
        if (b.length > size) {
            throw new IOException("Trying to write " + b.length + " bytes (" + toStringBinary(b) + ") into a field of length " + size);
        } else {
            out.writeBytes(s);

            for(int i = 0; i < size - s.length(); ++i) {
                out.writeByte(0);
            }

        }
    }

    public static String readStringFixedSize(DataInput in, int size) throws IOException {
        byte[] b = new byte[size];
        in.readFully(b);

        int n;
        for(n = b.length; n > 0 && b[n - 1] == 0; --n) {
        }

        return toString(b, 0, n);
    }

    public static byte[] copy(byte[] bytes) {
        if (bytes == null) {
            return null;
        } else {
            byte[] result = new byte[bytes.length];
            System.arraycopy(bytes, 0, result, 0, bytes.length);
            return result;
        }
    }

    public static byte[] copy(byte[] bytes, int offset, int length) {
        if (bytes == null) {
            return null;
        } else {
            byte[] result = new byte[length];
            System.arraycopy(bytes, offset, result, 0, length);
            return result;
        }
    }

    public static int unsignedBinarySearch(byte[] a, int fromIndex, int toIndex, byte key) {
        int unsignedKey = key & 255;
        int low = fromIndex;
        int high = toIndex - 1;

        while(low <= high) {
            int mid = low + high >>> 1;
            int midVal = a[mid] & 255;
            if (midVal < unsignedKey) {
                low = mid + 1;
            } else {
                if (midVal <= unsignedKey) {
                    return mid;
                }

                high = mid - 1;
            }
        }

        return -(low + 1);
    }

    public static byte[] unsignedCopyAndIncrement(byte[] input) {
        byte[] copy = copy(input);
        if (copy == null) {
            throw new IllegalArgumentException("cannot increment null array");
        } else {
            for(int i = copy.length - 1; i >= 0; --i) {
                if (copy[i] != -1) {
                    ++copy[i];
                    return copy;
                }

                copy[i] = 0;
            }

            byte[] out = new byte[copy.length + 1];
            out[0] = 1;
            System.arraycopy(copy, 0, out, 1, copy.length);
            return out;
        }
    }

    public static int indexOf(byte[] array, byte target) {
        for(int i = 0; i < array.length; ++i) {
            if (array[i] == target) {
                return i;
            }
        }

        return -1;
    }

    public static boolean contains(byte[] array, byte target) {
        return indexOf(array, target) > -1;
    }

    public static void random(byte[] b) {
        RNG.nextBytes(b);
    }

    public static byte[] createMaxByteArray(int maxByteCount) {
        byte[] maxByteArray = new byte[maxByteCount];

        for(int i = 0; i < maxByteArray.length; ++i) {
            maxByteArray[i] = -1;
        }

        return maxByteArray;
    }

    public static byte[] multiple(byte[] srcBytes, int multiNum) {
        if (multiNum <= 0) {
            return new byte[0];
        } else {
            byte[] result = new byte[srcBytes.length * multiNum];

            for(int i = 0; i < multiNum; ++i) {
                System.arraycopy(srcBytes, 0, result, i * srcBytes.length, srcBytes.length);
            }

            return result;
        }
    }
}
