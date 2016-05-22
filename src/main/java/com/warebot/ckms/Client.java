package com.warebot.ckms;

import com.warebot.ckms.stream.CKMSStream;
import com.warebot.ckms.stream.Stream;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Non-blocking thread-safe <code>Stream</code> client.
 * @param <T> Type of data being observed.
 */
final public class Client<T extends Number & Comparable<T>> {

    // Underlying Stream representing the streaming sample-set.
    final Stream<T> stream;

    // pendingFlush is an atomic counter representing a count of data points observed but not flush.
    final AtomicInteger pendingFlush = new AtomicInteger();

    // flushing is a flag that indicates whether the buffer is currently undergoing a flush.
    AtomicBoolean flushing = new AtomicBoolean(false);

    final ConcurrentLinkedQueue<T> buffer;
    final int bufCap;

    public Client(int bufCap) {
        buffer = new ConcurrentLinkedQueue<T>();
        stream = new CKMSStream<T>();
        this.bufCap = bufCap;
    }

    public Client(int bufCap, Stream<T> stream) {
        buffer = new ConcurrentLinkedQueue<T>();
        this.stream = stream;
        this.bufCap = bufCap;
    }

    public void observe(T val) {
        boolean observed = false;

        while(!observed) {
            if ((pendingFlush.incrementAndGet()) > bufCap) {
                if (flushing.compareAndSet(false, true)) {
                    // Buffer has been merged - clear it and reset pendingFlush counter.
                    merge(new ArrayList<T>(buffer));
                    pendingFlush.set(0);
                    buffer.clear();
                    buffer.add(val);
                    observed = true;
                    flushing.set(false);
                }
            } else {
                buffer.add(val);
                observed = true;
            }
        }
    }

    public void merge(){
        merge(new ArrayList<T>(buffer));
    }

    private void merge(List<T> buf) {
        stream.merge(buf);
    }
}
