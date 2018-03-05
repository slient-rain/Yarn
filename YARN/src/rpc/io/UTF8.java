package rpc.io;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.util.Locale;

public class UTF8 {
	private static final ThreadLocal<DataOutputBuffer> OBUF_FACTORY =
			new ThreadLocal<DataOutputBuffer>(){
		@Override
		protected DataOutputBuffer initialValue() {
			return new DataOutputBuffer();
		}
	};
	/** Write a UTF-8 encoded string.
	 *
	 * @see DataOutput#writeUTF(String)
	 */
	public static int writeString(DataOutput out, String s) throws IOException {
		if (s.length() > 0xffff/3) {         // maybe too long
			//	      LOG.warn("truncating long string: " + s.length()
			//	               + " chars, starting with " + s.substring(0, 20));
			s = s.substring(0, 0xffff/3);
		}

		int len = utf8Length(s);
		if (len > 0xffff)                             // double-check length
			throw new IOException("string too long!");

		out.writeShort(len);
		writeChars(out, s, 0, s.length());
		return len;
	}

	/** Read a UTF-8 encoded string.
	 *
	 * @see DataInput#readUTF()
	 */
	public static String readString(DataInput in) throws IOException {
		int bytes = in.readUnsignedShort();
		StringBuilder buffer = new StringBuilder(bytes);
		readChars(in, buffer, bytes);
		return buffer.toString();
	}

	private static void writeChars(DataOutput out,
			String s, int start, int length)
					throws IOException {
		final int end = start + length;
		for (int i = start; i < end; i++) {
			int code = s.charAt(i);
			if (code <= 0x7F) {
				out.writeByte((byte)code);
			} else if (code <= 0x07FF) {
				out.writeByte((byte)(0xC0 | ((code >> 6) & 0x1F)));
				out.writeByte((byte)(0x80 |   code       & 0x3F));
			} else {
				out.writeByte((byte)(0xE0 | ((code >> 12) & 0X0F)));
				out.writeByte((byte)(0x80 | ((code >>  6) & 0x3F)));
				out.writeByte((byte)(0x80 |  (code        & 0x3F)));
			}
		}
	}

	private static void readChars(DataInput in, StringBuilder buffer, int nBytes)
			throws UTFDataFormatException, IOException {
		DataOutputBuffer obuf = OBUF_FACTORY.get();
		obuf.reset();
		obuf.write(in, nBytes);
		byte[] bytes = obuf.getData();
		int i = 0;
		while (i < nBytes) {
			byte b = bytes[i++];
			if ((b & 0x80) == 0) {
				// 0b0xxxxxxx: 1-byte sequence
				buffer.append((char)(b & 0x7F));
			} else if ((b & 0xE0) == 0xC0) {
				if (i >= nBytes) {
					throw new UTFDataFormatException("Truncated UTF8 at " +
							byteToHexString(bytes, i - 1, 1));
				}
				// 0b110xxxxx: 2-byte sequence
				buffer.append((char)(((b & 0x1F) << 6)
						| (bytes[i++] & 0x3F)));
			} else if ((b & 0xF0) == 0xE0) {
				// 0b1110xxxx: 3-byte sequence
				if (i + 1 >= nBytes) {
					throw new UTFDataFormatException("Truncated UTF8 at " +
							byteToHexString(bytes, i - 1, 2));
				}
				buffer.append((char)(((b & 0x0F) << 12)
						| ((bytes[i++] & 0x3F) << 6)
						|  (bytes[i++] & 0x3F)));
			} else if ((b & 0xF8) == 0xF0) {
				if (i + 2 >= nBytes) {
					throw new UTFDataFormatException("Truncated UTF8 at " +
							byteToHexString(bytes, i - 1, 3));
				}
				// 0b11110xxx: 4-byte sequence
				int codepoint =
						((b & 0x07) << 18)
						| ((bytes[i++] & 0x3F) <<  12)
						| ((bytes[i++] & 0x3F) <<  6)
						| ((bytes[i++] & 0x3F));
				buffer.append(highSurrogate(codepoint))
				.append(lowSurrogate(codepoint));
			} else {
				// The UTF8 standard describes 5-byte and 6-byte sequences, but
				// these are no longer allowed as of 2003 (see RFC 3629)

				// Only show the next 6 bytes max in the error code - in case the
				// buffer is large, this will prevent an exceedingly large message.
				int endForError = Math.min(i + 5, nBytes);
				throw new UTFDataFormatException("Invalid UTF8 at " +
						byteToHexString(bytes, i - 1, endForError));
			}
		}
	}

	public static String byteToHexString(byte[] bytes, int start, int end) {
		if (bytes == null) {
			throw new IllegalArgumentException("bytes == null");
		}
		StringBuilder s = new StringBuilder(); 
		for(int i = start; i < end; i++) {
			s.append(format("%02x", bytes[i]));
		}
		return s.toString();
	}

	private static char highSurrogate(int codePoint) {
		return (char) ((codePoint >>> 10)
				+ (Character.MIN_HIGH_SURROGATE - (Character.MIN_SUPPLEMENTARY_CODE_POINT >>> 10)));
	}

	private static char lowSurrogate(int codePoint) {
		return (char) ((codePoint & 0x3ff) + Character.MIN_LOW_SURROGATE);
	}

	/** The same as String.format(Locale.ENGLISH, format, objects). */
	public static String format(final String format, final Object... objects) {
		return String.format(Locale.ENGLISH, format, objects);
	}

	/** Returns the number of bytes required to write this. */
	private static int utf8Length(String string) {
		int stringLength = string.length();
		int utf8Length = 0;
		for (int i = 0; i < stringLength; i++) {
			int c = string.charAt(i);
			if (c <= 0x007F) {
				utf8Length++;
			} else if (c > 0x07FF) {
				utf8Length += 3;
			} else {
				utf8Length += 2;
			}
		}
		return utf8Length;
	}
}
