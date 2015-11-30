import android.media.MediaCodec;
import android.media.MediaFormat;
import android.util.Log;

import com.hvt.horizon.HorizonApp;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Created by Petrakeas on 7/10/2015.
 */
public class CircularBuffer {
    private static final String TAG = "CircularBuffer";


    // Raw data (e.g. AVC NAL units) held here.
    //
    // The MediaMuxer writeSampleData() function takes a ByteBuffer.  If it's a "direct"
    // ByteBuffer it'll access the data directly, if it's a regular ByteBuffer it'll use
    // JNI functions to access the backing byte[] (which, in the current VM, is done without
    // copying the data).
    private ByteBuffer[] mDataBuffer;
    int mBuffersNum;
    private ByteOrder mOrder;
    private int mSpanMs;
    private int mTotalSpanMs;
    private int mBufferSize;
    private int mTotalBufferSize;

    // Meta-data held here.  We're using a collection of arrays, rather than an array of
    // objects with multiple fields, to minimize allocations and heap footprint.
    private int[] mPacketFlags;
    private long[] mPacketPtsUs;
    private int[] mPacketStart;
    private int[] mPacketLength;
    private int mMetaLength;
    private int mSingleBufferMetaLength;

    // Data is added at head and removed from tail.  Head points to an empty node, so if
    // head==tail the list is empty. We lose one slot with this convention because if we wrap around
    // we'll still need to keep one free slot for the head.
    private int mMetaHead;
    private int mMetaTail;

    private int mBitrate;
    private double mTimePerPacketMs;


    /**
     * Allocates the circular buffers we use for encoded data and meta-data.
     */
    public CircularBuffer(MediaFormat mediaFormat, int desiredSpanMs) {
        mBuffersNum = 1;
        mDataBuffer = new ByteBuffer[1];

        // For the encoded data, we assume the encoded bit rate is close to what we request.
        //
        // There would be a minor performance advantage to using a power of two here, because
        // not all ARM CPUs support integer modulus.
        mBitrate = mediaFormat.getInteger(MediaFormat.KEY_BIT_RATE);
        mBufferSize = (int)((long) mBitrate * desiredSpanMs / (8 * 1000));

        // Try to allocate mBufferSize bytes or at least minBufferSize bytes.
        int minBufferSize = mBitrate/8;
        mDataBuffer[0] = ByteBuffer.allocateDirect(mBufferSize);
        mBufferSize = mDataBuffer[0].capacity();
        mTotalBufferSize = mBufferSize;
        mSpanMs = (int) (((long) 1000 * 8 * mBufferSize)/(mBitrate));
        mTotalSpanMs = mSpanMs;

        // We want to calculate how many packets fit in our mBufferSize
        String mimeType = mediaFormat.getString(MediaFormat.KEY_MIME);
        boolean isVideo = mimeType.equals(MediaFormat.MIMETYPE_VIDEO_AVC);
        boolean isAudio = mimeType.equals(MediaFormat.MIMETYPE_AUDIO_AAC);
        double packetSize;
        double  packetsPerSecond;
        if (isVideo) {
            packetsPerSecond = mediaFormat.getInteger(MediaFormat.KEY_FRAME_RATE);
        }
        else if (isAudio) {
            double sampleRate = mediaFormat.getInteger(MediaFormat.KEY_SAMPLE_RATE);
            packetsPerSecond = sampleRate/1024;
        }
        else {
            throw new RuntimeException("Media format provided is neither AVC nor AAC");
        }
        mTimePerPacketMs =  1000./packetsPerSecond;
        packetSize = (mBitrate/packetsPerSecond)/8;
        int estimatedPacketCount = (int) (mBufferSize/packetSize + 1);
        // Meta-data is smaller than encoded data for non-trivial frames, so we over-allocate
        // a bit.  This should ensure that we drop packets because we ran out of (expensive)
        // data storage rather than (inexpensive) metadata storage.
        mMetaLength = estimatedPacketCount * 2;
        mSingleBufferMetaLength = mMetaLength;
        mPacketFlags = new int[mMetaLength];
        mPacketPtsUs = new long[mMetaLength];
        mPacketStart = new int[mMetaLength];
        mPacketLength = new int[mMetaLength];

        Log.d(TAG, "BitRate=" + mBitrate +
                " span=" + String.format("%,d", mSpanMs) + "msec" +
                " buffer size=" + String.format("%,d", mBufferSize / 1000) + "kB" +
                " packet count=" + mMetaLength);

    }

    public boolean increaseSize() {
        // allocate another buffer
        mDataBuffer = increaseArraySize(mDataBuffer, mDataBuffer.length, ByteBuffer[].class, 1);
        int lastBufferId = mDataBuffer.length - 1;
        try {
            mDataBuffer[lastBufferId] = ByteBuffer.allocateDirect(mBufferSize);
        }
        catch (OutOfMemoryError E) {
            Log.w(TAG, "Could not allocate memory to increase size.");
            return false;
        }
        if (mDataBuffer[lastBufferId].capacity() != mBufferSize) {
            throw new RuntimeException("Allocated size can't be different.");
        }
        mDataBuffer[lastBufferId].order(mOrder);
        mTotalBufferSize += mBufferSize;
        mTotalSpanMs += mSpanMs;

        // increase meta array size
        mPacketFlags = increaseArraySize(mPacketFlags, mMetaLength, int[].class, mSingleBufferMetaLength, mMetaTail, mMetaHead);
        mPacketPtsUs = increaseArraySize(mPacketPtsUs, mMetaLength, long[].class, mSingleBufferMetaLength, mMetaTail, mMetaHead);
        mPacketStart = increaseArraySize(mPacketStart, mMetaLength, int[].class, mSingleBufferMetaLength, mMetaTail, mMetaHead);
        mPacketLength = increaseArraySize(mPacketLength, mMetaLength, int[].class, mSingleBufferMetaLength, mMetaTail, mMetaHead);
        int packetsUsed = getPacketNum();
        mMetaLength += mSingleBufferMetaLength;
        mMetaHead = (mMetaTail + packetsUsed) % mMetaLength;

        // Move packets so that we don't wrap around the buffer.
        //TODO: instead of moving them one by one, move them by buffer. it's X10 faster
        int index = getFirstIndex();
        index = getNextIndex(index);        // tail packet is excluded
        boolean shouldMove = false;
        while (index >= 0) {
            if (mPacketStart[index] == 0) { // the packets from this point on should be moved
                shouldMove = true;
            }
            if (shouldMove) {
                move(index);
            }
            index = getNextIndex(index);
        }

        Log.d(TAG, "Buffer size increased. BitRate=" + mBitrate +
                " span=" + String.format("%,d", mTotalSpanMs) + "msec" +
                " buffer size=" + String.format("%,d", mTotalBufferSize / 1000) + "kB" +
                " packet count=" + mMetaLength);

        return true;
    }

    /**
     * Creates and returns a new array of arrayType with size: sourceSize + sizeIncrement. Also, it
     * copies all the elements of the source array to the new array without re-ordering them.
     */
    private static <A> A increaseArraySize (A sourceArray, int sourceSize, Class<A> arrayType, int sizeIncrement) {
        int newSize = sourceSize + sizeIncrement;
        A newArray = arrayType.cast(Array.newInstance(arrayType.getComponentType(), newSize));
        System.arraycopy(sourceArray, 0, newArray, 0, sourceSize);
        return newArray;
    }

    /**
     * Creates and returns a new array of arrayType with size: sourceSize + sizeIncrement. The elements
     * of the source array are copied to the new array in a new order according to the provided tail
     * and head index of the original array.
     *
     * The index of the tail remains the same. The following elements are added after until the head
     * is reached. The elements may be wrapped-around the new array.
     */
    private static <A> A increaseArraySize (A sourceArray, int sourceSize, Class<A> arrayType, int sizeIncrement, int tailIndex, int headIndex) {
        int newSize = sourceSize + sizeIncrement;
        A newArray = arrayType.cast(Array.newInstance(arrayType.getComponentType(), newSize));
        if (headIndex > tailIndex) { // source elements are not wrapped around
            System.arraycopy(sourceArray, tailIndex, newArray, tailIndex, headIndex - tailIndex);
        }
        else { // source elements are wrapped around
            System.arraycopy(sourceArray, tailIndex, newArray, tailIndex, sourceSize - tailIndex);
            int remainSize = headIndex - 0;
            if (remainSize <= sizeIncrement) { // can fit in newArray without wrapping around
                System.arraycopy(sourceArray, 0, newArray, sourceSize, remainSize);
            }
            else { // we have to wrap-around in the new array
                System.arraycopy(sourceArray, 0, newArray, sourceSize, sizeIncrement);
                int secondPartSize = remainSize - sizeIncrement;
                System.arraycopy(sourceArray, sizeIncrement, newArray, 0, secondPartSize);
            }
        }
        return newArray;
    }

    public boolean isEmpty() {
        return (mMetaHead == mMetaTail);
    }
    /**
     * Returns the index of the tail which corresponds to the oldest packet. Valid until the
     * next removeTail().
     */
    public int getFirstIndex() {
        if (isEmpty()) {
            return -1;
        }
        return mMetaTail;
    }

    /**
     * Returns the index of the next packet, or -1 if we've reached the end.
     */
    public int getNextIndex(int index) {
        int next = (index + 1) % mMetaLength;
        if (next == mMetaHead) {
            next = -1;
        }
        return next;
    }

    /**
     * Returns the index of the next packet, or -1 if we've passed the tail.
     */
    public int getPreviousIndex(int index) {
        if (index == mMetaTail) {
            return -1;
        }
        int previous = (index - 1 + mMetaLength) % mMetaLength;
        return previous;
    }

    /**
     * Returns the index before the head which corresponds to the newest packet (newest). Valid until
     * the next add() or increaseSize().
     */
    public int getLastIndex() {
        if (isEmpty()) {
            return -1;
        }
        return (mMetaHead + mMetaLength - 1) % mMetaLength;
    }

    /**
     * Returns the current total number of added packets.
     */
    public int getPacketNum() {
        // We don't count the +1 meta slot reserved for the head.
        int usedMeta = (mMetaHead + mMetaLength - mMetaTail) % mMetaLength ;
        return usedMeta;
    }

    private int getFreeMeta() {
        int packetNum = getPacketNum();
        // we subtrack 1 slot, for the space reserved for the head
        int freeMeta = mMetaLength - packetNum -1;
        return freeMeta;
    }

    /**
     * Computes the data buffer offset for the next place to store data.
     * <p/>
     * Equal to the start of the previous packet's data plus the previous packet's length.
     */
    private int getHeadStart() {
        if (isEmpty()) {
            return 0;
        }
        int beforeHead = getLastIndex();
        return (mPacketStart[beforeHead] + mPacketLength[beforeHead]) % mTotalBufferSize;
    }

    /**
     * Returns the free space from the specified headstart until the tail of the data buffer.
     */
    private int getFreeSpace(int headStart) {
        if (isEmpty()) {
            return mTotalBufferSize;
        }
        // Need the byte offset of the start of the "tail" packet, and the byte offset where
        // "head" will store its data.
        int tailStart = mPacketStart[mMetaTail];
        int freeSpace = (tailStart + mTotalBufferSize - headStart) % mTotalBufferSize;
        return freeSpace;
    }

    private int getUsedSpace() {
        if (isEmpty()) {
            return 0;
        }
        int freeSpace = getFreeSpace(getHeadStart());
        int usedSpace = mTotalBufferSize - freeSpace;
        return usedSpace;
    }

    /**
     * Computes the amount of time spanned by the buffered data, based on the presentation
     * time stamps.
     */
    private double computeTimeSpanMs() {
        if (isEmpty()) {
            return 0;
        }
        double timeSpan = 0;
        int index = getFirstIndex();
        while (index >= 0) {
            timeSpan += mTimePerPacketMs;
            index = getNextIndex(index);
        }

        return timeSpan;
    }

    private void printStatus() {
        int usedSpace = getUsedSpace();
        double usedSpacePercent = 100. * (double) usedSpace / mTotalBufferSize;
        String usedSpaceString = String.format("%.2f", usedSpacePercent);

        int usedMeta = getPacketNum();

        Log.v(TAG, "Used " + usedSpaceString + "% from  " + String.format("%,d", mTotalBufferSize / 1000) + "kB"
                + ", meta used=" + usedMeta + "/" + (mMetaLength - 1));
    }

    /**
     * Adds a new encoded data packet to the buffer.
     *
     * @return the index where the packet was stored or -1 if it failed to add the packet.
     */
    public int add(ByteBuffer buf, MediaCodec.BufferInfo info) {
        int size = info.size;
        if (HorizonApp.VERBOSE) {
            Log.d(TAG, "add size=" + size + " flags=0x" + Integer.toHexString(info.flags) +
                    " pts=" + info.presentationTimeUs);
        }

        if (mOrder == null) {
            mOrder = buf.order();
            for (int i = 0; i < mDataBuffer.length; i++) {
                mDataBuffer[i].order(mOrder);
            }
        }
        if (mOrder != buf.order()) {
            throw new RuntimeException("Byte ordering changed");
        }

        if (!canAdd(size)) {
            return -1;
        }

        int headStart = getHeadStart();
        // Check if we have to write to the beginning of the next/same data buffer.
        int bufferStart =  (headStart / mBufferSize) * mBufferSize;      // 0 for single buffer
        int bufferEnd = bufferStart + mBufferSize -1;
        if (headStart + size -1 > bufferEnd) {
            headStart = (bufferStart + mBufferSize) % mTotalBufferSize; // 0 for single buffer
        }

        int packetStart = headStart % mBufferSize;          // always 0 when changing buffer
        int bufferId = headStart / mBufferSize;                         // 0 for single buffer

        buf.limit(info.offset + info.size);
        buf.position(info.offset);
        mDataBuffer[bufferId].limit(packetStart + info.size);
        mDataBuffer[bufferId].position(packetStart);
        mDataBuffer[bufferId].put(buf);

        mPacketFlags[mMetaHead] = info.flags;
        mPacketPtsUs[mMetaHead] = info.presentationTimeUs;
        mPacketStart[mMetaHead] = headStart;
        mPacketLength[mMetaHead] = size;

        int currentIndex = mMetaHead;
        mMetaHead = (mMetaHead + 1) % mMetaLength;

        if (HorizonApp.VERBOSE) {
            printStatus();
        }

        return currentIndex;
    }

    /**
     * Determines whether this is enough space to fit "size" bytes in the data buffer, and
     * a packet in each meta-data array.
     *
     * @return True if there is enough space to add without removing anything.
     */
    private boolean canAdd(int size) {
        if (size > mBufferSize) {
            throw new RuntimeException("Enormous packet: " + size + " vs. buffer " +
                    mBufferSize);
        }
        if (isEmpty()) {
            if (HorizonApp.VERBOSE) {
                int headStart = getHeadStart();
                int freeSpace = getFreeSpace(headStart);
                Log.v(TAG, "OK headStart=" + String.format("%,d", headStart) +
                        " req=" + size + " free=" + freeSpace + ")");
            }
            return true;
        }

        // Make sure we can advance head without stepping on the tail.
        int nextHead = (mMetaHead + 1) % mMetaLength;
        if (nextHead == mMetaTail) {
            if (HorizonApp.VERBOSE) {
                Log.v(TAG, "Ran out of metadata (head=" + mMetaHead + " tail=" + mMetaTail + ")");
            }
            return false;
        }

        // Make sure we have enough free space in the data buffer.
        int headStart = getHeadStart();
        int freeSpace = getFreeSpace(headStart);
        if (size > freeSpace) {
            if (HorizonApp.VERBOSE) {
                int tailStart = mPacketStart[mMetaTail];
                Log.v(TAG, "Ran out of data (tailStart=" + tailStart + " headStart=" + headStart +
                        " req=" + size + " free=" + freeSpace + ")");
            }
            return false;
        }

        // Check if the packet can't fit until the end of its data buffer. If true, we'll write to
        // the beginning of the next/same data buffer. We need to check again for free space.
        int bufferStart =  (headStart / mBufferSize) * mBufferSize;     // 0 for single buffer
        int bufferEnd = bufferStart + mBufferSize -1;
        if (headStart + size -1 > bufferEnd) {
            headStart = (bufferStart + mBufferSize) % mTotalBufferSize; // 0 for single buffer
            freeSpace = getFreeSpace(headStart);
            if (size > freeSpace) {
                if (HorizonApp.VERBOSE) {
                    int tailStart = mPacketStart[mMetaTail];
                    Log.v(TAG, "Ran out of data (tailStart=" + String.format("%,d", tailStart) +
                            " headStart=" + String.format("%,d", headStart) +
                            " req=" + size + " free=" + freeSpace + ")");
                }
                return false;
            }
        }

        if (HorizonApp.VERBOSE) {
            int tailStart = mPacketStart[mMetaTail];
            Log.v(TAG, "OK (tailStart=" + String.format("%,d",tailStart) +
                    " headStart=" + String.format("%,d",headStart) +
                    " req=" + size + " free=" + freeSpace + ")");
        }

        return true;
    }

    /**
     * Moves the provided packet's position in the data buffer so that it is placed after the previous
     * packet position.
     */
    private void move(int index) {
        int previousIndex = getPreviousIndex(index);
        if (previousIndex == -1) {
            throw new RuntimeException("Can't move tail packet.");
        }
        int headStart = (mPacketStart[previousIndex] + mPacketLength[previousIndex]) % mTotalBufferSize;
        int size = mPacketLength[index];

        // Check if we have to write to the beginning of the next/same data buffer.
        int bufferStart =  (headStart / mBufferSize) * mBufferSize;
        int bufferEnd = bufferStart + mBufferSize -1;
        if (headStart + size -1 > bufferEnd) {
            headStart = (bufferStart + mBufferSize) % mTotalBufferSize;
        }

        int packetStart = headStart % mBufferSize;
        int bufferId = headStart / mBufferSize;

        MediaCodec.BufferInfo sourceInfo = new MediaCodec.BufferInfo();
        ByteBuffer sourceBuffer = getChunk(index, sourceInfo);
        mDataBuffer[bufferId].limit(packetStart + size);
        mDataBuffer[bufferId].position(packetStart);
        mDataBuffer[bufferId].put(sourceBuffer);

        mPacketStart[index] = headStart;
    }

    /**
     * Returns a reference to a "direct" ByteBuffer with the data, and fills in the
     * BufferInfo.
     * <p/>
     * The caller must not modify the contents of the returned ByteBuffer.  Altering
     * the position and limit is allowed.
     */
    public ByteBuffer getChunk(int index, MediaCodec.BufferInfo info) {
        if (isEmpty()) {
            throw new RuntimeException("Can't return chunk of empty buffer");
        }

        int packetStart = mPacketStart[index] % mBufferSize;
        int bufferId = mPacketStart[index] / mBufferSize;

        info.flags = mPacketFlags[index];
        info.presentationTimeUs = mPacketPtsUs[index];
        info.offset = packetStart;
        info.size = mPacketLength[index];

        ByteBuffer byteBuffer = mDataBuffer[bufferId].duplicate();
        byteBuffer.order(mOrder);
        byteBuffer.limit(info.offset + info.size);
        byteBuffer.position(info.offset);

        return byteBuffer;
    }

    public ByteBuffer getTailChunk(MediaCodec.BufferInfo info) {
        int index = getFirstIndex();
        return getChunk(index, info);
    }

    /**
     * Removes the tail packet.
     */
    public void removeTail() {
        if (HorizonApp.VERBOSE) {
            Log.d(TAG, "remove tail:" + mMetaTail + " pts=" + mPacketPtsUs[mMetaTail]);
        }
        if (isEmpty()) {
            throw new RuntimeException("Can't removeTail() in empty buffer");
        }
        mMetaTail = (mMetaTail + 1) % mMetaLength;

        if (HorizonApp.VERBOSE) {
            printStatus();
        }
    }

}
