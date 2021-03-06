/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.core.publisher;

import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiPredicate;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Exceptions;
import reactor.core.Producer;
import reactor.util.concurrent.QueueSupplier;

import static reactor.core.publisher.Operators.cancelledSubscription;

final class MonoSequenceEqual<T> extends Mono<Boolean> {
	final Publisher<? extends T>            first;
	final Publisher<? extends T>            second;
	final BiPredicate<? super T, ? super T> comparer;
	final int                               bufferSize;

	public MonoSequenceEqual(Publisher<? extends T> first, Publisher<? extends T> second,
			BiPredicate<? super T, ? super T> comparer, int bufferSize) {
		this.first = Objects.requireNonNull(first, "first");
		this.second = Objects.requireNonNull(second, "second");
		this.comparer = Objects.requireNonNull(comparer, "comparer");
		if(bufferSize < 1){
			throw new IllegalArgumentException("Buffer size must be strictly positive: " +
					""+bufferSize);
		}
		this.bufferSize = bufferSize;
	}

	@Override
	public void subscribe(Subscriber<? super Boolean> s) {
		EqualCoordinator<T> ec = new EqualCoordinator<>(s, bufferSize, first, second, comparer);
		ec.subscribe();
	}

	static final class EqualCoordinator<T> implements Subscription {
		final Subscriber<? super Boolean> actual;
		final BiPredicate<? super T, ? super T> comparer;
		final Publisher<? extends T> first;
		final Publisher<? extends T> second;
		final EqualSubscriber<T> firstSubscriber;
		final EqualSubscriber<T> secondSubscriber;

		volatile boolean cancelled;

		volatile int once;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<EqualCoordinator> ONCE =
				AtomicIntegerFieldUpdater.newUpdater(EqualCoordinator.class, "once");

		T v1;

		T v2;

		volatile int wip;
		@SuppressWarnings("rawtypes")
		static final AtomicIntegerFieldUpdater<EqualCoordinator> WIP =
				AtomicIntegerFieldUpdater.newUpdater(EqualCoordinator.class, "wip");

		public EqualCoordinator(Subscriber<? super Boolean> actual, int bufferSize,
				Publisher<? extends T> first, Publisher<? extends T> second,
				BiPredicate<? super T, ? super T> comparer) {
			this.actual = actual;
			this.first = first;
			this.second = second;
			this.comparer = comparer;
			firstSubscriber = new EqualSubscriber<>(this, bufferSize);
			secondSubscriber = new EqualSubscriber<>(this, bufferSize);
		}

		void subscribe() {
			first.subscribe(firstSubscriber);
			second.subscribe(secondSubscriber);
		}

		@Override
		public void request(long n) {
			if (!Operators.validate(n)) {
				return;
			}
			if (ONCE.compareAndSet(this, 0, 1)) {
				first.subscribe(firstSubscriber);
				second.subscribe(secondSubscriber);
			}
		}

		@Override
		public void cancel() {
			if (!cancelled) {
				cancelled = true;
				Subscription s = firstSubscriber.subscription;
				if (s != cancelledSubscription()) {
					s = EqualSubscriber.S.getAndSet(firstSubscriber,
							cancelledSubscription());
					if (s != null && s != cancelledSubscription()) {
						s.cancel();
					}
				}
				s = secondSubscriber.subscription;
				if (s != cancelledSubscription()) {
					s = EqualSubscriber.S.getAndSet(firstSubscriber,
							cancelledSubscription());
					if (s != null && s != cancelledSubscription()) {
						s.cancel();
					}
				}

				if (WIP.getAndIncrement(this) == 0) {
					firstSubscriber.queue.clear();
					secondSubscriber.queue.clear();
				}
			}
		}

		void cancel(Queue<T> q1, Queue<T> q2) {
			cancelled = true;
			q1.clear();
			q2.clear();
		}

		void drain() {
			if (WIP.getAndIncrement(this) != 0) {
				return;
			}

			int missed = 1;
			final EqualSubscriber<T> s1 = firstSubscriber;
			final Queue<T> q1 = s1.queue;
			final EqualSubscriber<T> s2 = secondSubscriber;
			final Queue<T> q2 = s2.queue;

			for (;;) {

				long r = 0L;
				for (;;) {
					if (cancelled) {
						q1.clear();
						q2.clear();
						return;
					}

					boolean d1 = s1.done;

					if (d1) {
						Throwable e = s1.error;
						if (e != null) {
							cancel(q1, q2);

							actual.onError(e);
							return;
						}
					}

					boolean d2 = s2.done;

					if (d2) {
						Throwable e = s2.error;
						if (e != null) {
							cancel(q1, q2);

							actual.onError(e);
							return;
						}
					}

					if (v1 == null) {
						v1 = q1.poll();
					}
					boolean e1 = v1 == null;

					if (v2 == null) {
						v2 = q2.poll();
					}
					boolean e2 = v2 == null;

					if (d1 && d2 && e1 && e2) {
						actual.onNext(true);
						actual.onComplete();
						return;
					}
					if ((d1 && d2) && (e1 != e2)) {
						cancel(q1, q2);

						actual.onNext(false);
						actual.onComplete();
						return;
					}

					if (!e1 && !e2) {
						boolean c;

						try {
							c = comparer.test(v1, v2);
						} catch (Throwable ex) {
							Exceptions.throwIfFatal(ex);
							cancel(q1, q2);

							actual.onError(Operators.onOperatorError(ex));
							return;
						}

						if (!c) {
							cancel(q1, q2);

							actual.onNext(false);
							actual.onComplete();
							return;
						}
						r++;

						v1 = null;
						v2 = null;
					}

					if (e1 || e2) {
						break;
					}
				}

				if (r != 0L) {
					s1.cachedSubscription.request(r);
					s2.cachedSubscription.request(r);
				}


				missed = WIP.addAndGet(this, -missed);
				if (missed == 0) {
					break;
				}
			}
		}
	}

	static final class EqualSubscriber<T>
			implements Subscriber<T>, Producer {
		final EqualCoordinator<T> parent;
		final Queue<T>            queue;
		final int                 bufferSize;

		volatile boolean done;
		Throwable error;

		Subscription cachedSubscription;
		volatile Subscription subscription;
		static final AtomicReferenceFieldUpdater<EqualSubscriber, Subscription> S =
				AtomicReferenceFieldUpdater.newUpdater(EqualSubscriber.class,
						Subscription.class, "subscription");

		public EqualSubscriber(EqualCoordinator<T> parent, int bufferSize) {
			this.parent = parent;
			this.bufferSize = bufferSize;
			this.queue = QueueSupplier.<T>get(bufferSize).get();
		}

		@Override
		public Object downstream() {
			return parent;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.setOnce(S, this, s)) {
				this.cachedSubscription = s;
				s.request(bufferSize);
			}
		}

		@Override
		public void onNext(T t) {
			if (!queue.offer(t)) {
				onError(Exceptions.failWithOverflow("Queue full?!"));
				return;
			}
			parent.drain();
		}

		@Override
		public void onError(Throwable t) {
			error = t;
			done = true;
			parent.drain();
		}

		@Override
		public void onComplete() {
			done = true;
			parent.drain();
		}
	}
}
