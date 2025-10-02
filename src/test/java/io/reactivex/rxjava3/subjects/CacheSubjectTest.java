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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import io.reactivex.rxjava3.core.Observer;
import io.reactivex.rxjava3.core.RxJavaTest;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.observers.TestObserver;
import io.reactivex.rxjava3.schedulers.Schedulers;
import io.reactivex.rxjava3.testsupport.Reclaimable;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.*;

public class CacheSubjectTest extends RxJavaTest {

    private final ExecutorService executor = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r);
        t.setName("myThread");
        return t;
    });

    private final Reclaimable<Payload> s1 = Reclaimable.of(Payload.of("s1"));
    private final Reclaimable<Payload> s2 = Reclaimable.of(Payload.of("s2"));
    private final Reclaimable<Payload> s3 = Reclaimable.of(Payload.of("s3"));
    private final Reclaimable<Payload> s4 = Reclaimable.of(Payload.of("s4"));
    private final Reclaimable<Payload> s5 = Reclaimable.of(Payload.of("s5"));
    private final Reclaimable<Payload> s6 = Reclaimable.of(Payload.of("s6"));
    private final Reclaimable<Payload> s7 = Reclaimable.of(Payload.of("s7"));
    private final Reclaimable<Payload> s8 = Reclaimable.of(Payload.of("s8"));

    @After
    public void tearDown() throws Throwable {
        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }

    @Test
    public void subscribersArriveAtDifferentPointsInTheSubjectLifecycle() {
        // arrange
        Reclaimable<CacheSubject<Payload>> subject = Reclaimable.of("subject", CacheSubject.create(2));

        // act
        Observer<Payload> observer = subject.referent().observer();
        observer.onSubscribe(Disposable.empty());
        TestObserver<String> testObserver1 = subject.referent().map(Payload::value).test();
        observer.onNext(s1.remove());
        TestObserver<String> testObserver2 = subject.referent().map(Payload::value).test();
        observer.onNext(s2.remove());
        TestObserver<String> testObserver3 = subject.referent().map(Payload::value).test();
        observer.onComplete();
        TestObserver<String> testObserver4 = subject.referent().map(Payload::value).test();
        subject.remove();

        // verify
        Reclaimable.forceGC().assertReclaimed(subject).assertReclaimed(s1).assertReclaimed(s2);
        testObserver1.assertValues("s1", "s2").assertComplete();
        testObserver2.assertValues("s1", "s2").assertComplete();
        testObserver3.assertValues("s1", "s2").assertComplete();
        testObserver4.assertValues("s1", "s2").assertComplete();
    }

    @Test
    public void payloadsBecomeReclaimedAsSubscriberAdvances() {
        // arrange
        Reclaimable<CacheSubject<Payload>> subject = Reclaimable.of("subject", CacheSubject.create(3));
        Observer<Payload> observer = subject.referent().observer();

        // act-verify
        observer.onNext(s1.remove());
        observer.onNext(s2.remove());
        observer.onNext(s3.remove());
        observer.onNext(s4.remove()); // second node is created, s1+s2+s3 become reclaimable
        TestObserver<String> testObserver = subject.remove()
                .map(Payload::value)
                .test()
                .assertValues("s1", "s2", "s3", "s4");
        Reclaimable.forceGC()
                .assertReclaimed(subject)
                .assertReclaimed(s1)
                .assertReclaimed(s2)
                .assertReclaimed(s3)
                .assertUnreclaimed(s4);
        observer.onNext(s5.remove());
        observer.onNext(s6.remove());
        Reclaimable.forceGC()
                .assertUnreclaimed(s4)
                .assertUnreclaimed(s5)
                .assertUnreclaimed(s6);
        observer.onNext(s7.remove()); // third node is created, s4+s5+s6 become reclaimable
        observer.onNext(s8.remove());
        Reclaimable.forceGC()
                .assertReclaimed(s4)
                .assertReclaimed(s5)
                .assertReclaimed(s6)
                .assertUnreclaimed(s7)
                .assertUnreclaimed(s8);
        observer.onComplete(); // third node is now full, and the observer should have nulled out its reference
        testObserver.assertResult("s1", "s2", "s3", "s4",  "s5", "s6", "s7", "s8");
        Reclaimable.forceGC().assertReclaimed(s7).assertReclaimed(s8);
    }

    @Test
    public void upstreamAddsAnotherSignalToTheActiveNode_whileDownstreamIsReplaying() throws Throwable {
        // arrange
        CountDownLatch arrive = new CountDownLatch(1);
        CountDownLatch depart = new CountDownLatch(1);
        Reclaimable<CacheSubject<Payload>> subject = Reclaimable.of("subject", CacheSubject.create(3));
        Observer<Payload> observer = subject.referent().observer();

        // act-verify
        observer.onNext(s1.remove());
        Future<TestObserver<String>> testObserver = executor.submit(() -> subject.remove()
                .map(Payload::value)
                .doOnNext(s -> {
                    if ("s1".equals(s)) {
                        arrive.countDown();
                        depart.await();
                    }
                })
                .test());
        arrive.await();
        observer.onNext(s2.remove());
        depart.countDown();
        testObserver.get().awaitCount(2).assertValues("s1", "s2");
    }

    @Test
    public void upstreamAddsMultipleNodes_whileDownstreamIsReplaying() throws Throwable {
        // arrange
        CountDownLatch arrive1 = new CountDownLatch(1);
        CountDownLatch depart1 = new CountDownLatch(1);
        CountDownLatch arrive6 = new CountDownLatch(1);
        CountDownLatch depart6 = new CountDownLatch(1);
        Reclaimable<CacheSubject<Payload>> subject = Reclaimable.of("subject", CacheSubject.create(2));
        Observer<Payload> observer = subject.referent().observer();

        // act-verify
        observer.onNext(s1.remove());
        observer.onNext(s2.remove()); // first node is now full
        Future<TestObserver<String>> f = executor.submit(() -> subject.remove()
                .map(Payload::value)
                .doOnNext(s -> {
                    if ("s1".equals(s)) {
                        arrive1.countDown();
                        depart1.await();
                    }
                    if ("s6".equals(s)) {
                        arrive6.countDown();
                        depart6.await();
                    }
                })
                .test());
        arrive1.await();
        observer.onNext(s3.remove());
        observer.onNext(s4.remove()); // second node is now full
        observer.onNext(s5.remove());
        depart1.countDown();
        TestObserver<String> testObserver = f.get(); // ensures the subscribe()'ing side thread is fully unwound
        testObserver.awaitCount(5);
        executor.execute(() -> {
            observer.onNext(s6.remove()); // third node is now full
            observer.onNext(s7.remove());
            observer.onNext(s8.remove()); // fourth node is now full
            observer.onComplete();
        });
        arrive6.await();
        Reclaimable.forceGC()
                .assertReclaimed(subject)
                .assertReclaimed(s1) // node 1
                .assertReclaimed(s2) // node 1
                .assertReclaimed(s3) // node 2
                .assertReclaimed(s4) // node 2
                .assertUnreclaimed(s5) // node 3
                .assertUnreclaimed(s6) // node 3
                .assertUnreclaimed(s7) // node 4
                .assertUnreclaimed(s8); // node 4
        depart6.countDown();
        testObserver.await().assertResult("s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8");
        Reclaimable.forceGC()
                .assertReclaimed(s5)
                .assertReclaimed(s6)
                .assertReclaimed(s7)
                .assertReclaimed(s8);
    }

    @Test
    public void cleanupWhileNotifyingDownstream_threadSwitchAfterPayloadDiscarded() throws Throwable {
        // arrange
        CountDownLatch arrive = new CountDownLatch(1);
        CountDownLatch depart = new CountDownLatch(1);
        Reclaimable<CacheSubject<Payload>> subject = Reclaimable.of("subject", CacheSubject.create(1));
        Observer<Payload> observer = subject.referent().observer();

        // act-verify
        TestObserver<String> testObserver = subject.remove()
                .map(Payload::value)
                .observeOn(Schedulers.io())
                .doAfterNext($ -> {
                    arrive.countDown();
                    depart.await();
                })
                .test();
        observer.onSubscribe(Disposable.empty());
        observer.onNext(s1.remove());
        arrive.await();
        Reclaimable.forceGC().assertReclaimed(subject).assertUnreclaimed(s1);
        observer.onComplete();
        Reclaimable.forceGC().assertReclaimed(s1);
        testObserver.assertValues("s1").assertNotComplete().assertNoErrors();
        depart.countDown();
        testObserver.await().assertValues("s1").assertComplete();
    }

    @Test
    public void onNextOnComplete_arriveWhileDownstreamIsDraining() throws Throwable {
        // arrange
        CountDownLatch arrive = new CountDownLatch(1);
        CountDownLatch depart = new CountDownLatch(1);
        CacheSubject<String> subject = CacheSubject.create();
        Observer<String> observer = subject.observer();

        // act
        observer.onNext("foo");
        Future<TestObserver<String>> vf = executor.submit(() -> subject.doOnNext(s -> {
            if ("foo".equals(s)) {
                arrive.countDown();
                depart.await();
            }
        }).test());
        arrive.await();
        observer.onNext("bar");
        observer.onComplete();
        depart.countDown();

        // verify
        vf.get().await().assertResult("foo", "bar");
    }

    @Test
    public void onNextOnError_arriveWhileDownstreamIsDraining() throws Throwable {
        // arrange
        CountDownLatch arrive = new CountDownLatch(1);
        CountDownLatch depart = new CountDownLatch(1);
        Throwable error = new Throwable("error");
        CacheSubject<String> subject = CacheSubject.create();
        Observer<String> observer = subject.observer();

        // act
        observer.onNext("foo");
        Future<TestObserver<String>> vf = executor.submit(() -> subject.doOnNext(s -> {
            if ("foo".equals(s)) {
                arrive.countDown();
                depart.await();
            }
        }).test());
        arrive.await();
        observer.onNext("bar");
        observer.onError(error);
        depart.countDown();

        // verify
        vf.get().await().assertValues("foo", "bar").assertError(error);
    }

    @Test
    public void disposeDuringOnSubscribe() {
        // arrange
        CacheSubject<String> subject = CacheSubject.create();

        // act
        Observer<String> observer = subject.observer();
        TestObserver<String> testObserver = subject.doOnSubscribe(Disposable::dispose).test();
        observer.onNext("foo");

        // verify
        testObserver.assertNoValues().assertNotComplete().assertNoErrors();
    }

    @Test
    public void disposeDuringOnNext() throws Throwable {
        // arrange
        CountDownLatch arrive = new CountDownLatch(1);
        CountDownLatch depart = new CountDownLatch(1);
        AtomicReference<Disposable> disposable = new AtomicReference<>();
        Reclaimable<CacheSubject<Payload>> subject = Reclaimable.of("subject", CacheSubject.create(3));
        Observer<Payload> observer = subject.referent().observer();

        // act
        observer.onNext(s1.remove());
        observer.onNext(s2.remove());
        Future<TestObserver<String>> f = executor.submit(() -> subject.remove().map(Payload::value).doOnNext(s -> {
            if ("s1".equals(s)) {
                arrive.countDown();
                depart.await();
                disposable.get().dispose();
            }
        }).doOnSubscribe(disposable::set).test());
        arrive.await();
        observer.onNext(s3.remove());
        depart.countDown();
        TestObserver<String> testObserver = f.get(); // ensures the subscribe()'ing side thread is fully unwound
        testObserver.assertValues("s1").assertNotComplete().assertNoErrors();
    }

    @Test
    public void disposalOfMultipleDownstreams() {
        // arrange
        CacheSubject<String> subject = CacheSubject.create();
        Disposable[] disposables = new Disposable[4];
        Observer<String> observer = subject.observer();
        TestObserver<String> downstream1 = subject.doOnSubscribe(d -> disposables[0] = d).test();
        TestObserver<String> downstream2 = subject.doOnSubscribe(d -> disposables[1] = d).test();
        TestObserver<String> downstream3 = subject.doOnSubscribe(d -> disposables[2] = d).test();
        TestObserver<String> downstream4 = subject.doOnSubscribe(d -> disposables[3] = d).test();

        // act
        disposables[0].dispose();
        observer.onNext("foo");
        disposables[3].dispose();
        observer.onNext("bar");
        disposables[1].dispose();
        observer.onComplete();
        disposables[2].dispose();

        // verify
        assertTrue(disposables[0].isDisposed());
        assertTrue(disposables[1].isDisposed());
        assertTrue(disposables[2].isDisposed());
        assertTrue(disposables[3].isDisposed());
        downstream1.assertNoValues().assertNotComplete().assertNoErrors();
        downstream4.assertValues("foo").assertNotComplete().assertNoErrors();
        downstream2.assertValues("foo", "bar").assertNotComplete().assertNoErrors();
        downstream3.assertValues("foo", "bar").assertComplete();
    }

    @Test
    public void downstreamDisposesMultipleTimes() {
        // arrange
        AtomicReference<Disposable> disposable = new AtomicReference<>();
        CacheSubject<String> subject = CacheSubject.create();
        Observer<String> observer = subject.observer();

        // act-verify
        TestObserver<String> testObserver = subject.doOnSubscribe(disposable::set).test();
        observer.onNext("foo");
        assertFalse(disposable.get().isDisposed());
        disposable.get().dispose();
        observer.onNext("bar");
        assertTrue(disposable.get().isDisposed());
        disposable.get().dispose();
        testObserver.assertValues("foo").assertNotComplete().assertNoErrors();
    }

    @Test
    public void uncontendedDisposal_doesNotPreventReclamation() {
        // arrange
        AtomicReference<Disposable> disposable = new AtomicReference<>();
        Reclaimable<CacheSubject<Payload>> subject = Reclaimable.of("subject", CacheSubject.create(2));
        Observer<Payload> observer = subject.referent().observer();

        // act
        TestObserver<String> testObserver1 = subject.referent().doOnSubscribe(disposable::set).map(Payload::value).test();
        TestObserver<String> testObserver2 = subject.remove().map(Payload::value).test();
        observer.onNext(s1.remove());
        observer.onNext(s2.remove());
        observer.onNext(s3.remove());
        Reclaimable.forceGC()
                .assertReclaimed(subject)
                .assertReclaimed(s1)
                .assertReclaimed(s2)
                .assertUnreclaimed(s3);
        disposable.get().dispose();
        observer.onNext(s4.remove());
        observer.onNext(s5.remove());
        observer.onComplete();
        testObserver1.assertValues("s1", "s2", "s3").assertNotComplete().assertNoErrors();
        Reclaimable.forceGC()
                .assertReclaimed(subject)
                .assertReclaimed(s3)
                .assertReclaimed(s4)
                .assertReclaimed(s5);
        testObserver2.assertResult("s1", "s2", "s3", "s4", "s5");
    }

    @Test
    public void subjectReceivesOnSubscribeAfterTermination() {
        // arrange
        Disposable disposable = new Disposable() {
            private boolean disposed;
            @Override
            public void dispose() {
                disposed = true;
            }

            @Override
            public boolean isDisposed() {
                return disposed;
            }
        };
        CacheSubject<String> subject = CacheSubject.create();

        // act
        Observer<String> observer = subject.observer();
        TestObserver<String> testObserver = subject.test();
        observer.onNext("foo");
        observer.onComplete();
        observer.onSubscribe(disposable);

        // verify
        testObserver.assertResult("foo");
        assertTrue(disposable.isDisposed());
    }

    @Test
    public void subjectReceivesOnNextAfterTermination() {
        // arrange
        CacheSubject<String> subject = CacheSubject.create();

        // act
        Observer<String> observer = subject.observer();
        TestObserver<String> testObserver = subject.test();
        observer.onNext("foo");
        observer.onComplete();
        observer.onNext("bar");

        // verify
        testObserver.assertResult("foo");
    }

    @Test
    public void subjectReceivesOnCompleteAfterTermination() {
        // arrange
        CacheSubject<String> subject = CacheSubject.create();

        // act
        Observer<String> observer = subject.observer();
        TestObserver<String> testObserver = subject.test();
        observer.onNext("foo");
        observer.onComplete();
        observer.onComplete();

        // verify
        testObserver.assertResult("foo");
    }

    @Test
    public void subjectReceivesOnErrorAfterTermination() {
        // arrange
        Throwable error = new Throwable("error");
        CacheSubject<String> subject = CacheSubject.create();

        // act
        Observer<String> observer = subject.observer();
        TestObserver<String> testObserver = subject.test();
        observer.onNext("foo");
        observer.onComplete();
        observer.onError(error);

        // verify
        testObserver.assertResult("foo");
    }

    @Test
    public void parameterValidation() {
        assertThrows(IllegalArgumentException.class, () -> CacheSubject.create(-1));
        assertThrows(IllegalArgumentException.class, () -> CacheSubject.create(0));
        CacheSubject.create(1);
    }

    static final class Payload {
        private final String value;
        private Payload(String value) {
            this.value = value;
        }
        static Payload of(String value) {
            return new Payload(value);
        }

        String value() {
            return value;
        }

        @Override
        public String toString() {
            return "Payload(" + value + ")";
        }
    }
}
