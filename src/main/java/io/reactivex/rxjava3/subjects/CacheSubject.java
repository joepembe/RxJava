/*
 * Copyright (c) 2016-present, RxJava Contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
 * the License for the specific language governing permissions and limitations under the License.
 */

package io.reactivex.rxjava3.subjects;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReference;

import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Observer;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.internal.util.ExceptionHelper;
import io.reactivex.rxjava3.internal.util.NotificationLite;
import io.reactivex.rxjava3.plugins.RxJavaPlugins;

/**
 * {@code CacheSubject} is a GC-friendly {@code Subject} with behavior that resembles an unbounded
 * {@link ReplaySubject}: {@code CacheSubject} offers both "read" ({@link Observable}) and
 * "write" ({@link Observer}) interfaces, and all signals written to it are cached internally so that they may be
 * replayed to late-arriving downstreams. However, unlike {@code ReplaySubject}, when the "read" interface of a
 * {@code CacheSubject} instance goes out of scope (i.e. becomes garbage), cached signals that have already been
 * replayed to all downstreams also go out of scope, thereby making it possible for buffered {@code T} values to be
 * reclaimed by the garbage collector.
 * <p>
 * To accomplish this, {@code CacheSubject} implements its "read" and "write" interfaces as separate objects. The "read"
 * interface is {@code Observable} (which {@code CacheSubject} directly {@code extend}s), and the "write" interface is
 * {@code Observer}, and is obtained by calling {@link #observer()}. Signals are cached in a singly-linked list that
 * remains in scope for as long as the {@code CacheSubject} itself remains in scope, and becomes progressively
 * unreachable once the {@code CacheSubject} goes out of scope and any already-subscribed downstreams walk forwards
 * through the linked list.
 * <p>
 * The intuition here is: as soon as the "read" interface (i.e. the {@code CacheSubject} instance) is no longer strongly
 * reachable, it becomes impossible for new downstreams to arrive and attempt to read cached notifications from start to
 * finish. Hence, as downstreams progress through the linked list, nodes whose contents have been fully replayed become
 * unreachable and eligible for garbage collection.
 *
 * @param <T>
 *         the type of values accepted and emitted by the {@code CacheSubject}
 */
public final class CacheSubject<T> extends Observable<T> {

    private static final int DEFAULT_NODE_CAPACITY = 4;

    @SuppressWarnings("rawtypes")
    private static final CacheDisposable[] EMPTY = new CacheDisposable[0];

    @SuppressWarnings("rawtypes")
    private static final CacheDisposable[] TERMINATED = new CacheDisposable[0];

    /**
     * The first node in a singly linked list. Each node in this list has the capacity to hold a specific number of
     * notifications, and each points exclusively to the <em>next</em> node (there are no backwards references). When a
     * new subscriber arrives, the subscription is initialized with the "head" node, and any notifications present in
     * the linked list are replayed. As the newly-arrived subscriber replays buffered items, it advances through the
     * linked list, discarding each node reference once it has replayed all items in that node. Consequently, when
     * {@code this} instance goes out of scope, the region of the linked list starting at this head node and ending at
     * the node that precedes first node that is still being replayed by a subscriber becomes unreachable and eligible
     * for collection.
     */
    private final Node head;

    private final Multicaster<T> multicaster;

    private CacheSubject(int nodeCapacity) {
        Node node = new Node(nodeCapacity);
        this.head = node;
        this.multicaster = new Multicaster<>(nodeCapacity, node);
    }

    /**
     * Creates an unbounded {@code CacheSubject} with a {@link #DEFAULT_NODE_CAPACITY default buffer capacity}.
     *
     * @param <T>
     *         the type of values accepted and emitted by the {@code CacheSubject}
     * @return a new {@code CacheSubject} instance with a default buffer capacity.
     */
    public static <T> @NonNull CacheSubject<T> create() {
        return create(DEFAULT_NODE_CAPACITY);
    }

    /**
     * Creates an unbounded {@code CacheSubject} with the specified node capacity. Node capacity determines how many
     * cached notifications can be contained in each node in the linked list, and can influence when those
     * notifications actually become reclaimable: cached notifications go out of scope in "batches" of size
     * {@code nodeCapacity}.
     * <p>
     * On choosing a value for {@code nodeCapacity}:
     * <ul>
     *   <li>
     *       Larger values for {@code nodeCapacity} improve memory locality and reduce the number of supporting objects
     *       that are constructed, but at the potential cost of retaining as many as {@code nodeCapacity} references to
     *       {@code T} values for longer than necessary. This is because {@code T} references held in the same node of
     *       {@code CacheSubject}'s underlying linked list only become reclaimable once every subscriber has replayed
     *       every notification in that node.
     *   </li>
     *   <li>
     *       Smaller values for {@code nodeCapacity} reduce memory locality and increase the number of supporting
     *       objects that are constructed, but may increase the efficiency at which individual {@code T} values become
     *       reclaimable.
     *   </li>
     * </ul>
     *
     * @param nodeCapacity
     *         The number of cached notifications that can be held in each node in the linked list (must be greater than
     *         {@code 0}). Choose larger values (e.g. 16, 256, etc.) for streams that emit very many and/or lightweight
     *         {@code T} values, and smaller values (e.g. 2, 1) for streams that emit relatively few and/or heavyweight
     *         {@code T} values.
     * @param <T>
     *         the type of values accepted and emitted by the {@code CacheSubject}
     * @return a new {@code CacheSubject} instance the specified node capacity
     * @throws IllegalArgumentException
     *         if {@code nodeCapacity} is less than {@code 1}
     */
    public static <T> @NonNull CacheSubject<T> create(int nodeCapacity) {
        if (nodeCapacity <= 0) {
            throw new IllegalArgumentException("nodeCapacity must be greater than 0");
        }
        return new CacheSubject<>(nodeCapacity);
    }

    /**
     * Returns the "write" interface for this subject. Consumers are expected to interact with the returned
     * {@code Observer} in strict accordance with the Reactive Streams specification. In particular, calls to
     * {@link Observer#onNext(Object)}, {@link Observer#onError(Throwable)} and {@link Observer#onComplete()} are
     * required to be serialized (called from the same thread or called non-overlappingly from different threads through
     * external means of serialization).
     *
     * @return the "write" interface for this subject.
     */
    public @NonNull Observer<T> observer() {
        return multicaster;
    }

    @Override
    protected void subscribeActual(@NonNull Observer<? super T> observer) {
        multicaster.subscribe(observer, head);
    }

    static final class Multicaster<T> implements Observer<T> {

        @SuppressWarnings("unchecked")
        private static final AtomicIntegerFieldUpdater<Multicaster<?>> SIZE = AtomicIntegerFieldUpdater.newUpdater(
                (Class<Multicaster<?>>) (Class<?>) Multicaster.class, "size");

        private final AtomicReference<CacheDisposable<T>[]> observers;
        private final int nodeCapacity;

        /**
         * The last node in the linked list. As notifications arrive, they are either appended to this node (if it has
         * enough capacity), or appended to a newly-created node (if the current tail node is full).
         */
        private Node tail;

        /**
         * The total number of notifications (onNext, onComplete, onError) that have been appended.
         */
        private volatile int size;

        /**
         * How many items have been put into the tail node so far.
         */
        private int tailOffset;

        private boolean done;

        @SuppressWarnings("unchecked")
        Multicaster(int nodeCapacity, Node head) {
            this.nodeCapacity = nodeCapacity;
            this.tail = head;
            // the last write must be to a 'final' field to ensure that the object can be safely published
            this.observers = new AtomicReference<>((CacheDisposable<T>[]) EMPTY);
        }

        void subscribe(Observer<? super T> observer, Node head) {
            CacheDisposable<T> cd = new CacheDisposable<>(observer, this, head);
            observer.onSubscribe(cd);
            if (add(cd)) {
                if (cd.cancelled) {
                    remove(cd);
                    return;
                }
            }
            cd.replay();
        }

        @Override
        public void onSubscribe(@NonNull Disposable d) {
            ExceptionHelper.nullCheck(d, "onSubscribe called with a null value.");
            if (observers.get() == TERMINATED) {
                d.dispose();
            }
        }

        @Override
        public void onNext(@NonNull T t) {
            ExceptionHelper.nullCheck(t, "onNext called with a null value.");
            if (done) {
                return;
            }
            appendNotification(t, false);
            for (CacheDisposable<T> cd : observers.get()) {
                cd.replay();
            }
        }

        @Override
        public void onError(@NonNull Throwable t) {
            ExceptionHelper.nullCheck(t, "onError called with a null Throwable.");
            if (done) {
                RxJavaPlugins.onError(t);
                return;
            }
            done = true;
            appendNotification(NotificationLite.error(t), true);
            for (CacheDisposable<T> cd : terminateObservers()) {
                cd.replay();
            }
        }

        @Override
        public void onComplete() {
            if (done) {
                return;
            }
            done = true;
            appendNotification(NotificationLite.complete(), true);
            for (CacheDisposable<T> cd : terminateObservers()) {
                cd.replay();
            }
        }

        @SuppressWarnings("unchecked")
        private CacheDisposable<T>[] terminateObservers() {
            return observers.getAndSet(TERMINATED);
        }

        private void appendNotification(Object notification, boolean terminal) {
            int offset = tailOffset;
            if (offset == nodeCapacity) {
                // The current tail node is full, so append another node and add the notification to the new node
                Node node = new Node(offset); // offset == nodeCapacity (avoids another field read)
                node.notifications[0] = notification;
                tailOffset = 1;
                tail.next = node;
                tail = node;
            } else {
                // The current tail node is not full, so add the notification to the tail node
                tail.notifications[offset] = notification;
                tailOffset = offset + 1;
            }
            if (terminal) {
                // No additional notifications will arrive, so now we can clear the 'tail' reference. This ensures that
                // once all downstreams have caught up, the tail node and any elements it contains can be reclaimed.
                tail = null;
            }
            SIZE.incrementAndGet(this);
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        private boolean add(CacheDisposable<T> cd) {
            for (;;) {
                CacheDisposable<T>[] prev = observers.get();
                if (prev == TERMINATED) {
                    return false;
                }
                int len = prev.length;
                CacheDisposable<T>[] next = new CacheDisposable[len + 1];
                System.arraycopy(prev, 0, next, 0, len);
                next[len] = cd;
                if (observers.compareAndSet(prev, next)) {
                    return true;
                }
            }
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        private void remove(CacheDisposable<T> cd) {
            for (;;) {
                CacheDisposable<T>[] prev = observers.get();
                if (prev == TERMINATED || prev == EMPTY) {
                    return;
                }
                int len = prev.length;
                int index = -1;
                for (int i = 0; i < len; i++) {
                    if (prev[i] == cd) {
                        index = i;
                        break;
                    }
                }

                if (index < 0) {
                    return;
                }
                CacheDisposable<T>[] next;
                if (len == 1) {
                    next = EMPTY;
                } else {
                    next = new CacheDisposable[len - 1];
                    System.arraycopy(prev, 0, next, 0, index);
                    System.arraycopy(prev, index + 1, next, index, len - index - 1);
                }
                if (observers.compareAndSet(prev, next)) {
                    return;
                }
            }
        }
    }

    static final class CacheDisposable<T> implements Disposable {

        @SuppressWarnings("unchecked")
        private static final AtomicIntegerFieldUpdater<CacheDisposable<?>> ENTRANCES = AtomicIntegerFieldUpdater.newUpdater(
                (Class<CacheDisposable<?>>) (Class<?>) CacheDisposable.class, "entrances");

        private final Observer<? super T> downstream;
        private final Multicaster<T> parent;

        /**
         * The current node in the linked list that this subscription is replaying.
         */
        private Node node;
        /**
         * The offset into the current node of the next notification to replay.
         */
        private int offset;
        /**
         * The total number of notifications that this subscription has replayed.
         */
        private int index;

        private volatile boolean cancelled;
        private volatile int entrances;

        CacheDisposable(Observer<? super T> actual, Multicaster<T> parent, Node head) {
            this.downstream = actual;
            this.parent = parent;
            this.node = head;
        }

        @Override
        public void dispose() {
            if (!cancelled) {
                cancelled = true;
                parent.remove(this);
                // We now need to null out the 'node' field, but must do so without invalidating any invariants depended
                // upon by the replay() loop. This is because another thread may be currently executing the replay()
                // loop, and we don't want to scatter null checks throughout replay() or have the racing thread
                // potentially undo our write. Hence, we:
                //
                //   (1) set cancelled to true, and
                //   (2) attempt to enter replay()
                //
                // If we successfully enter replay(), then we will immediately check 'cancelled' and set 'node' to null
                // on our way out.
                //
                // If we did not successfully enter replay(), then the replay()-ing thread will eventually also check
                // 'cancelled', and set 'node' to null on its way out.
                replay();
                //this.node = null;
            }
        }

        @Override
        public boolean isDisposed() {
            return cancelled;
        }

        void replay() {
            if (ENTRANCES.getAndIncrement(this) != 0) {
                return;
            }

            int missed = 1;
            final int nodeCapacity = parent.nodeCapacity;
            final Observer<? super T> downstream = this.downstream;
            int index = this.index;
            int offset = this.offset;
            Node node = this.node;

            do {
                for (;;) {
                    if (this.cancelled) {
                        this.node = null;
                        return;
                    }

                    // volatile read, establishes a happens-before with the writes performed in notifyObservers()
                    int size = parent.size;
                    if (index == size) {
                        break;
                    }

                    if (offset == nodeCapacity) {
                        node = node.next;
                        offset = 0;
                    }
                    Object notification = node.notifications[offset];
                    index++;
                    offset++;

                    if (NotificationLite.isComplete(notification)) {
                        downstream.onComplete();
                        this.cancelled = true;
                        this.node = null;
                        return;
                    }
                    if (NotificationLite.isError(notification)) {
                        downstream.onError(NotificationLite.getError(notification));
                        this.cancelled = true;
                        this.node = null;
                        return;
                    }

                    @SuppressWarnings("unchecked") T t = (T) notification;
                    downstream.onNext(t);
                }

                this.index = index;
                this.offset = offset;
                this.node = node;

                missed = ENTRANCES.addAndGet(this, -missed);
            } while (missed > 0);
        }
    }

    /**
     * Represents a segment of the cached item list as part of a linked-node-list structure.
     */
    static final class Node {

        /**
         * The array of notifications (values / NotificationLite instances) held by this node.
         */
        private final Object[] notifications;

        /**
         * The next node.
         */
        private volatile Node next;

        Node(int nodeCapacity) {
            this.notifications = new Object[nodeCapacity];
        }
    }
}
